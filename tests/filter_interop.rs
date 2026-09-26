use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use trzsz_rs::filter::{ProgressEvent, TrzszFilter, TrzszOptions};

#[derive(Clone)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum InputMessage {
    Shutdown,
}

struct ClientInput(Receiver<InputMessage>);

impl Read for ClientInput {
    fn read(&mut self, _output: &mut [u8]) -> io::Result<usize> {
        let _ = self.0.recv();
        Ok(0)
    }
}

fn build_go_tool(temp: &Path, name: &str) -> PathBuf {
    let go_module = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("trzsz-go");
    let output = temp.join(format!("go-{name}"));
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&output)
        .arg(format!("./cmd/{name}"))
        .current_dir(go_module)
        .status()
        .expect("Go toolchain is required for filter interoperability tests");
    assert!(status.success(), "failed to build Go {name}");
    output
}

fn spawn_go_server(
    executable: &Path,
    args: &[&Path],
    directory_mode: bool,
) -> (Child, ChildStdin, ChildStdout) {
    let mut command = Command::new(executable);
    command.args(["-q", "-y"]);
    if directory_mode {
        command.arg("-d");
    }
    for arg in args {
        command.arg(arg);
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start Go trzsz command");
    let stdin = child.stdin.take().expect("Go stdin pipe");
    let stdout = child.stdout.take().expect("Go stdout pipe");
    (child, stdin, stdout)
}

fn start_filter(
    server_in: ChildStdin,
    server_out: ChildStdout,
    output: Arc<Mutex<Vec<u8>>>,
) -> (Arc<TrzszFilter>, Sender<InputMessage>) {
    let (input_sender, input_receiver) = mpsc::channel();
    let filter = Arc::new(TrzszFilter::new(
        Box::new(ClientInput(input_receiver)),
        Box::new(CaptureWriter(output)),
        Box::new(server_in),
        Box::new(server_out),
        TrzszOptions {
            terminal_columns: 80,
            ..Default::default()
        },
    ));
    let shutdown_sender = input_sender.clone();
    filter.set_shutdown_handlers(
        Some(Arc::new(move || {
            let _ = shutdown_sender.send(InputMessage::Shutdown);
        })),
        None,
    );
    (filter, input_sender)
}

fn run_filter(filter: &Arc<TrzszFilter>) -> JoinHandle<io::Result<()>> {
    let running = filter.clone();
    thread::spawn(move || running.run())
}

fn wait_for_process(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll Go process") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("Go transfer command timed out");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn finish_filter(
    mut child: Child,
    run_thread: JoinHandle<io::Result<()>>,
    timeout: Duration,
) -> ExitStatus {
    let status = wait_for_process(&mut child, timeout);
    let run_result = run_thread.join().expect("join filter run loop");
    assert!(run_result.is_ok(), "filter run failed: {run_result:?}");
    status
}

fn assert_callback_transitions(states: &Arc<Mutex<Vec<bool>>>) {
    assert_eq!(&*states.lock().unwrap(), &[true, false]);
}

#[test]
fn rust_filter_transfers_files_both_directions_and_directories_with_go() {
    let temp = tempfile::tempdir().unwrap();
    let go_tsz = build_go_tool(temp.path(), "tsz");
    let go_trz = build_go_tool(temp.path(), "trz");

    // Go tsz -> Rust filter download (S trigger), with default destination path.
    let download_dir = temp.path().join("downloads");
    std::fs::create_dir(&download_dir).unwrap();
    let source = temp.path().join("go-source.bin");
    let expected = (0..512 * 1024).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    std::fs::write(&source, &expected).unwrap();
    let mut go_source = Command::new(&go_tsz)
        .arg(&source)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start Go tsz");
    let go_stdin = go_source.stdin.take().unwrap();
    let go_stdout = go_source.stdout.take().unwrap();
    let output = Arc::new(Mutex::new(Vec::new()));
    let (filter, _input) = start_filter(go_stdin, go_stdout, output.clone());
    filter.set_default_download_path(&download_dir);
    let states = Arc::new(Mutex::new(Vec::new()));
    let state_capture = states.clone();
    filter.set_transfer_state_callback(move |active| {
        state_capture.lock().unwrap().push(active);
    });
    let progress = Arc::new(Mutex::new(Vec::<ProgressEvent>::new()));
    let progress_capture = progress.clone();
    filter.set_progress_callback(move |event| {
        progress_capture.lock().unwrap().push(event);
    });
    let redraws = Arc::new(Mutex::new(0usize));
    let redraw_capture = redraws.clone();
    filter.set_redraw_screen_func(move || {
        *redraw_capture.lock().unwrap() += 1;
    });
    let run_thread = run_filter(&filter);
    let status = finish_filter(go_source, run_thread, Duration::from_secs(30));
    assert!(status.success(), "Go tsz failed: {status}");
    assert_eq!(
        std::fs::read(download_dir.join("go-source.bin")).unwrap(),
        expected
    );
    assert!(String::from_utf8_lossy(&output.lock().unwrap()).contains("\x1b[?25h"));
    assert_callback_transitions(&states);
    assert_eq!(*redraws.lock().unwrap(), 1);
    let progress = progress.lock().unwrap();
    assert!(progress.iter().any(|event| event.file_count == 1));
    assert!(
        progress
            .iter()
            .any(|event| event.file_name == "go-source.bin")
    );
    assert!(progress.iter().any(|event| event.done));

    // Rust filter -> Go trz single-file upload (R trigger, host path selector).
    let upload_dir = temp.path().join("received-files");
    std::fs::create_dir(&upload_dir).unwrap();
    let upload = temp.path().join("rust-source.bin");
    let upload_expected = vec![b'R'; 300 * 1024 + 7];
    std::fs::write(&upload, &upload_expected).unwrap();
    let picker_start = temp.path().join("upload-picker-start");
    let suggested_path = Arc::new(Mutex::new(None));
    let capture_suggested = suggested_path.clone();
    let selected_upload = upload.clone();
    let (go_receiver, go_stdin, go_stdout) = spawn_go_server(&go_trz, &[&upload_dir], false);
    let (filter, _input) = start_filter(go_stdin, go_stdout, Arc::new(Mutex::new(Vec::new())));
    filter.set_default_upload_path(&picker_start);
    filter.set_upload_path_selector(move |directory, suggested| {
        assert!(!directory);
        *capture_suggested.lock().unwrap() = suggested;
        Ok(Some(vec![selected_upload.clone()]))
    });
    let run_thread = run_filter(&filter);
    let status = finish_filter(go_receiver, run_thread, Duration::from_secs(30));
    assert!(status.success(), "Go trz file receive failed: {status}");
    assert_eq!(*suggested_path.lock().unwrap(), Some(picker_start));
    assert_eq!(
        std::fs::read(upload_dir.join("rust-source.bin")).unwrap(),
        upload_expected
    );

    // Rust filter -> Go trz -d directory upload (D trigger, V4 archive flow).
    let directory = temp.path().join("rust-directory");
    std::fs::create_dir_all(directory.join("nested")).unwrap();
    std::fs::write(
        directory.join("nested/data.bin"),
        b"directory payload\0\x03\xff",
    )
    .unwrap();
    std::fs::create_dir_all(directory.join("empty")).unwrap();
    let directory_expected = std::fs::read(directory.join("nested/data.bin")).unwrap();
    let receive_dir = temp.path().join("received-directory");
    std::fs::create_dir(&receive_dir).unwrap();
    let (go_receiver, go_stdin, go_stdout) = spawn_go_server(&go_trz, &[&receive_dir], true);
    let (filter, _input) = start_filter(go_stdin, go_stdout, Arc::new(Mutex::new(Vec::new())));
    let completion = filter
        .one_time_upload(std::slice::from_ref(&directory))
        .unwrap();
    let run_thread = run_filter(&filter);
    let status = finish_filter(go_receiver, run_thread, Duration::from_secs(30));
    assert!(
        status.success(),
        "Go trz directory receive failed: {status}"
    );
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(3)).unwrap(),
        Ok(())
    );
    assert_eq!(
        std::fs::read(receive_dir.join("rust-directory/nested/data.bin")).unwrap(),
        directory_expected
    );
    assert!(receive_dir.join("rust-directory/empty").is_dir());
    // Host-cancelled upload sends an unconfirmed ACT to a waiting Go trz.
    let cancelled_upload_dir = temp.path().join("cancelled-upload");
    std::fs::create_dir(&cancelled_upload_dir).unwrap();
    let (go_receiver, go_stdin, go_stdout) =
        spawn_go_server(&go_trz, &[&cancelled_upload_dir], false);
    let (filter, _input) = start_filter(go_stdin, go_stdout, Arc::new(Mutex::new(Vec::new())));
    let upload_picker_called = Arc::new(Mutex::new(false));
    let upload_picker_state = upload_picker_called.clone();
    filter.set_upload_path_selector(move |directory, _suggested| {
        assert!(!directory);
        *upload_picker_state.lock().unwrap() = true;
        Ok(None)
    });
    let run_thread = run_filter(&filter);
    let status = finish_filter(go_receiver, run_thread, Duration::from_secs(30));
    assert!(status.success(), "Go trz cancellation failed: {status}");
    assert!(*upload_picker_called.lock().unwrap());
    assert_eq!(std::fs::read_dir(&cancelled_upload_dir).unwrap().count(), 0);

    // A host-cancelled download sends an unconfirmed ACT and exits cleanly.
    let mut go_source = Command::new(&go_tsz)
        .arg("-q")
        .arg(&source)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start Go tsz cancellation probe");
    let go_stdin = go_source.stdin.take().unwrap();
    let go_stdout = go_source.stdout.take().unwrap();
    let (filter, _input) = start_filter(go_stdin, go_stdout, Arc::new(Mutex::new(Vec::new())));
    let cancelled = Arc::new(Mutex::new(false));
    let cancel_state = cancelled.clone();
    filter.set_download_path_selector(move |suggested| {
        assert!(suggested.is_none());
        *cancel_state.lock().unwrap() = true;
        Ok(None)
    });
    let run_thread = run_filter(&filter);
    let status = finish_filter(go_source, run_thread, Duration::from_secs(30));
    assert!(status.success(), "Go tsz cancellation failed: {status}");
    assert!(*cancelled.lock().unwrap());
}
