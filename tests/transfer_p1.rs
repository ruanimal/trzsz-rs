use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use md5::{Digest, Md5};
use trzsz_rs::comm::SourceFile;
use trzsz_rs::escape;
use trzsz_rs::progress::ProgressCallback;
use trzsz_rs::transfer::TrzszTransfer;

#[derive(Default)]
struct RecordingProgress {
    steps: Vec<i64>,
    done: usize,
    sizes: Vec<i64>,
}

impl ProgressCallback for RecordingProgress {
    fn on_num(&mut self, _num: i64) {}
    fn on_name(&mut self, _name: &str) {}
    fn on_size(&mut self, size: i64) {
        self.sizes.push(size);
    }
    fn on_step(&mut self, step: i64) {
        self.steps.push(step);
    }
    fn on_done(&mut self) {
        self.done += 1;
    }
    fn set_pre_size(&mut self, _size: i64) {}
    fn set_pause(&mut self, _pausing: bool) {}
}

fn source_file(path: PathBuf, name: &str, size: usize) -> SourceFile {
    SourceFile {
        path_id: 0,
        abs_path: path,
        rel_path: vec![name.to_string()],
        is_dir: false,
        archive: false,
        sub_files: Vec::new(),
        size: size as i64,
        perm: None,
    }
}

fn source_directory(path_id: i32, name: &str, perm: Option<u32>) -> SourceFile {
    SourceFile {
        path_id,
        abs_path: PathBuf::new(),
        rel_path: vec![name.to_string()],
        is_dir: true,
        archive: false,
        sub_files: Vec::new(),
        size: 0,
        perm,
    }
}

fn source_nested_file(path_id: i32, directory: &str, name: &str, perm: Option<u32>) -> SourceFile {
    SourceFile {
        path_id,
        abs_path: PathBuf::new(),
        rel_path: vec![directory.to_string(), name.to_string()],
        is_dir: false,
        sub_files: Vec::new(),
        archive: false,
        size: 0,
        perm,
    }
}

fn name_frame(source: &SourceFile) -> Vec<u8> {
    format!(
        "#NAME:{}\n",
        escape::encode_string(&serde_json::to_string(source).unwrap())
    )
    .into_bytes()
}

#[test]
fn send_and_receive_report_progress_for_each_data_chunk() {
    let data = vec![0x5a; 4096];
    let digest = Md5::digest(&data);

    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.bin");
    std::fs::write(&source_path, &data).unwrap();

    let acknowledgements = format!(
        "#SUCC:1\n#SUCC:{}\n#SUCC:{}\n#SUCC:1024\n#SUCC:2048\n#SUCC:1024\n#SUCC:{}\n",
        escape::encode_string("source.bin"),
        data.len(),
        escape::encode_bytes(&digest)
    );
    let mut sender = TrzszTransfer::new(Box::new(io::sink()));
    sender.add_received_data(acknowledgements.as_bytes(), false);
    let mut progress = RecordingProgress::default();
    let mut callback: Option<&mut dyn ProgressCallback> = Some(&mut progress);
    sender
        .send_files(
            &[source_file(source_path, "source.bin", data.len())],
            &mut callback,
        )
        .unwrap();

    assert_eq!(progress.sizes, vec![4096]);
    assert_eq!(progress.steps, vec![1024, 3072, 4096]);
    assert_eq!(progress.done, 1);

    let destination = tempfile::tempdir().unwrap();
    let mut protocol = format!(
        "#NUM:1\n#NAME:{}\n#SIZE:{}\n",
        escape::encode_string("received.bin"),
        data.len()
    );
    for chunk in [&data[..1024], &data[1024..3072], &data[3072..]] {
        protocol.push_str(&format!("#DATA:{}\n", escape::encode_bytes(chunk)));
    }
    protocol.push_str(&format!("#MD5:{}\n", escape::encode_bytes(&digest)));

    let mut receiver = TrzszTransfer::new(Box::new(io::sink()));
    receiver.add_received_data(protocol.as_bytes(), false);
    let mut progress = RecordingProgress::default();
    let mut callback: Option<&mut dyn ProgressCallback> = Some(&mut progress);
    receiver
        .recv_files(destination.path(), &mut callback)
        .unwrap();

    assert_eq!(progress.sizes, vec![4096]);
    assert_eq!(progress.steps, vec![1024, 3072, 4096]);
    assert_eq!(progress.done, 1);
    assert_eq!(
        std::fs::read(destination.path().join("received.bin")).unwrap(),
        data
    );
}

#[cfg(unix)]
#[test]
fn directory_transfer_preserves_permissions_and_uses_go_defaults() {
    use std::os::unix::fs::PermissionsExt;

    let destination = tempfile::tempdir().unwrap();
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.transfer_config.directory = true;

    let root = source_directory(1, "bundle", Some(0o751));
    transfer.add_received_data(&name_frame(&root), false);
    transfer.recv_file_name(destination.path()).unwrap();
    let file = source_nested_file(1, "bundle", "run.sh", Some(0o751));
    transfer.add_received_data(&name_frame(&file), false);
    drop(transfer.recv_file_name(destination.path()).unwrap().0);

    let root_mode = std::fs::metadata(destination.path().join("bundle"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let file_mode = std::fs::metadata(destination.path().join("bundle/run.sh"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(root_mode, 0o751 | 0o700);
    assert_eq!(file_mode, 0o751 | 0o600);

    let default_dir = source_directory(2, "private", None);
    transfer.add_received_data(&name_frame(&default_dir), false);
    transfer.recv_file_name(destination.path()).unwrap();
    let default_file = source_nested_file(2, "private", "data", None);
    transfer.add_received_data(&name_frame(&default_file), false);
    drop(transfer.recv_file_name(destination.path()).unwrap().0);

    let default_dir_mode = std::fs::metadata(destination.path().join("private"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let default_file_mode = std::fs::metadata(destination.path().join("private/data"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(default_dir_mode, 0o700);
    assert_eq!(default_file_mode, 0o600);
}

#[test]
fn stop_and_delete_removes_files_and_directories_but_plain_stop_keeps_files() {
    let destination = tempfile::tempdir().unwrap();
    let directory = destination.path().join("bundle");
    std::fs::create_dir_all(directory.join("nested")).unwrap();
    std::fs::write(directory.join("nested/file.bin"), b"payload").unwrap();
    let file = destination.path().join("plain.bin");
    std::fs::write(&file, b"payload").unwrap();

    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.add_created_files(directory.to_str().unwrap());
    transfer.add_created_files(file.to_str().unwrap());
    transfer.stop_transferring_files(true);
    let error = transfer.check_stop().unwrap_err();
    assert_eq!(error.message, "Stopped and deleted");
    transfer.client_error(&error);
    assert!(!directory.exists());
    assert!(!file.exists());

    let retained = destination.path().join("retained.bin");
    std::fs::write(&retained, b"keep").unwrap();
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.add_created_files(retained.to_str().unwrap());
    transfer.stop_transferring_files(false);
    let error = transfer.check_stop().unwrap_err();
    transfer.client_error(&error);
    assert!(retained.exists());
}

#[test]
fn cancellation_wakes_a_buffer_read_without_a_protocol_timeout() {
    let transfer = TrzszTransfer::new(Box::new(io::sink()));
    let stop = transfer.buffer.stop_handle();
    let (done_tx, done_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut transfer = transfer;
        let result = transfer.recv_line("ACT", false, None);
        done_tx.send(result.map_err(|error| error.message)).unwrap();
    });

    thread::sleep(Duration::from_millis(80));
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let result = done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("cancelled read should wake promptly");
    assert!(result.unwrap_err().contains("Stopped"));
    reader.join().unwrap();
}
