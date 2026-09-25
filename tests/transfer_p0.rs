use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use md5::{Digest, Md5};
use trzsz_rs::comm::{CompressType, FileReader, FileWriter, SourceFile};
use trzsz_rs::escape;
use trzsz_rs::transfer::{TransferAction, TrzszTransfer};

#[derive(Clone, Default)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct EofReader;

impl FileReader for EofReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }

    fn size(&self) -> i64 {
        1000
    }

    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct VecWriter(Vec<u8>);

impl FileWriter for VecWriter {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn latin1_escape_config_roundtrips_and_escapes_binary_payload() {
    let chars = escape::get_escape_chars(true);
    let json = serde_json::Value::Array(
        chars
            .iter()
            .map(|(from, to)| {
                serde_json::json!([escape::bytes_to_latin1(from), escape::bytes_to_latin1(to)])
            })
            .collect(),
    );
    let wire_json = serde_json::to_string(&json).unwrap();
    let decoded_json: serde_json::Value = serde_json::from_str(&wire_json).unwrap();
    let table = escape::escape_chars_to_table(decoded_json.as_array().unwrap()).unwrap();

    let raw = [0x1b, 0x03, 0x0d, 0x8d, 0x90, 0x91, 0x93, 0x9d, 0xee, b'~'];
    let escaped = escape::escape_data(&raw, &table);
    assert_ne!(escaped, raw);
    assert_eq!(
        escape::unescape_data(&escaped, &table, None).unwrap().0,
        raw
    );

    let invalid = serde_json::json!([["�", "�"]]);
    assert!(escape::escape_chars_to_table(invalid.as_array().unwrap()).is_err());
}

#[test]
fn binary_data_wire_frame_uses_latin1_escape_table() {
    let output = SharedWriter::default();
    let captured = output.0.clone();
    let mut sender = TrzszTransfer::new(Box::new(output));
    sender.transfer_config.binary = true;
    sender.transfer_config.escape_chars = Some(serde_json::Value::Array(
        escape::get_escape_chars(true)
            .iter()
            .map(|(from, to)| {
                serde_json::json!([escape::bytes_to_latin1(from), escape::bytes_to_latin1(to)])
            })
            .collect(),
    ));
    let raw = [0x1b, 0x03, 0x0d, 0x8d, 0x90, 0x91, 0x93, 0x9d, 0xee, b'~'];
    let table = escape::escape_chars_to_table(
        sender
            .transfer_config
            .escape_chars
            .as_ref()
            .unwrap()
            .as_array()
            .unwrap(),
    )
    .unwrap();
    let escaped = escape::escape_data(&raw, &table);
    sender.send_data(&raw).unwrap();
    let wire = captured.lock().unwrap().clone();
    assert!(
        wire.starts_with(format!("#DATA:{}\n", escaped.len()).as_bytes()),
        "wire={wire:?}"
    );

    let mut receiver = TrzszTransfer::new(Box::new(io::sink()));
    receiver.transfer_config.binary = true;
    receiver.transfer_config.escape_chars = sender.transfer_config.escape_chars.clone();
    receiver.add_received_data(&wire, false);
    assert_eq!(receiver.recv_data().unwrap(), raw);
}

#[test]
fn early_source_eof_returns_error_without_sending_empty_data() {
    let output = SharedWriter::default();
    let captured = output.0.clone();
    let mut transfer = TrzszTransfer::new(Box::new(output));

    let error = transfer.send_file_data(&mut EofReader).unwrap_err();
    assert!(error.message.contains("Unexpected EOF"));
    assert!(captured.lock().unwrap().is_empty());
}

#[test]
fn empty_data_before_negotiated_size_returns_error_without_spinning() {
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.transfer_config.timeout = 1;
    transfer.transfer_config.binary = false;
    transfer.add_received_data(
        format!("#DATA:{}\n", escape::encode_bytes(&[])).as_bytes(),
        false,
    );
    let mut output = VecWriter::default();

    let error = transfer.recv_file_data(&mut output, 1000).unwrap_err();
    assert!(error.message.contains("Unexpected empty DATA chunk"));
    assert!(output.0.is_empty());
}

#[test]
fn malformed_colon_lines_are_errors_not_panics() {
    for line in [b":oops\n".as_slice(), b"#\n", b"#:\n", b"\xffoops\n"] {
        let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
        transfer.add_received_data(line, false);
        assert!(transfer.recv_check("ACT", true, None).is_err());
    }
}

#[test]
fn junk_and_tmux_status_output_resynchronize_to_expected_message() {
    let payload = escape::encode_string("action");
    let input = format!(
        "noise\x1bP=100\x1bP=status line\x1b\\garbage#ACT:{}\n",
        payload
    );
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.transfer_config.tmux_output_junk = true;
    transfer.add_received_data(input.as_bytes(), false);

    assert_eq!(transfer.recv_string("ACT", false, None).unwrap(), "action");
}

#[test]
fn tmux_output_junk_is_included_in_cfg() {
    let output = SharedWriter::default();
    let captured = output.0.clone();
    let mut transfer = TrzszTransfer::new(Box::new(output));
    transfer.transfer_config.tmux_output_junk = true;

    transfer
        .send_config(
            true,
            false,
            false,
            false,
            &serde_json::Value::Null,
            0,
            &TransferAction::default(),
            CompressType::Auto,
        )
        .unwrap();

    let line = captured.lock().unwrap().clone();
    let value = std::str::from_utf8(&line).unwrap();
    let encoded = value.strip_prefix("#CFG:").unwrap().trim_end();
    let config: serde_json::Value =
        serde_json::from_slice(&escape::decode_string(encoded).unwrap()).unwrap();
    assert_eq!(config["tmux_output_junk"], true);
}

#[test]
fn binary_transfer_rejects_malformed_escape_config_instead_of_using_empty_table() {
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.transfer_config.binary = true;
    transfer.transfer_config.escape_chars = Some(serde_json::json!([["�", "�"]]));
    let error = transfer.send_data(&[0xee]).unwrap_err();
    assert!(error.message.contains("Escape chars invalid"));
}

#[test]
fn zero_byte_file_finishes_without_data_and_has_empty_md5() {
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    let mut output = VecWriter::default();
    let digest = transfer.recv_file_data(&mut output, 0).unwrap();
    assert_eq!(digest, Md5::digest([]).to_vec());
    assert!(output.0.is_empty());
}

#[test]
fn duplicate_file_name_is_renamed_without_overwriting() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("source.txt"), b"original").unwrap();
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.transfer_config.overwrite = false;
    transfer.add_received_data(
        format!("#NAME:{}\n", escape::encode_string("source.txt")).as_bytes(),
        false,
    );

    let (file, name) = transfer.recv_file_name(dir.path()).unwrap();
    assert_eq!(name, "source.txt.0");
    drop(file);
    assert_eq!(
        std::fs::read(dir.path().join("source.txt")).unwrap(),
        b"original"
    );
    assert!(dir.path().join("source.txt.0").exists());
}

#[test]
fn directory_upload_creates_nested_paths_and_rejects_empty_path_name() {
    let dir = tempfile::tempdir().unwrap();
    let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
    transfer.transfer_config.directory = true;

    let root = SourceFile {
        path_id: 0,
        abs_path: PathBuf::new(),
        rel_path: vec!["bundle".to_string()],
        is_dir: true,
        archive: false,
        size: 0,
        perm: None,
    };
    transfer.add_received_data(
        format!(
            "#NAME:{}\n",
            escape::encode_string(&serde_json::to_string(&root).unwrap())
        )
        .as_bytes(),
        false,
    );
    assert_eq!(transfer.recv_file_name(dir.path()).unwrap().1, "bundle");

    let nested = SourceFile {
        path_id: 0,
        abs_path: PathBuf::new(),
        rel_path: vec![
            "bundle".to_string(),
            "nested".to_string(),
            "a.txt".to_string(),
        ],
        is_dir: false,
        archive: false,
        size: 3,
        perm: None,
    };
    transfer.add_received_data(
        format!(
            "#NAME:{}\n",
            escape::encode_string(&serde_json::to_string(&nested).unwrap())
        )
        .as_bytes(),
        false,
    );
    let (mut file, name) = transfer.recv_file_name(dir.path()).unwrap();
    assert_eq!(name, "bundle");
    let mut file = file.take().unwrap();
    file.write_all(b"abc").unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("bundle/nested/a.txt")).unwrap(),
        b"abc"
    );

    let malformed = SourceFile {
        rel_path: vec![],
        ..nested
    };
    transfer.add_received_data(
        format!(
            "#NAME:{}\n",
            escape::encode_string(&serde_json::to_string(&malformed).unwrap())
        )
        .as_bytes(),
        false,
    );
    let error = transfer.recv_file_name(dir.path()).err().unwrap();
    assert!(error.message.contains("empty path_name"));
}

fn spawn_rs_process(
    name: &str,
    args: &[&str],
) -> (
    std::process::Child,
    std::process::ChildStdin,
    Arc<Mutex<Vec<u8>>>,
) {
    spawn_rs_process_with_env(name, args, &[])
}

fn spawn_rs_process_with_env(
    name: &str,
    args: &[&str],
    envs: &[(&str, std::ffi::OsString)],
) -> (
    std::process::Child,
    std::process::ChildStdin,
    Arc<Mutex<Vec<u8>>>,
) {
    use std::process::{Command, Stdio};

    let mut exe = std::env::current_exe().unwrap();
    exe.pop();
    exe.pop();
    exe.push(name);
    let mut command = Command::new(exe);
    command
        .args(args)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().unwrap();
    let stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let output = Arc::new(Mutex::new(Vec::new()));
    let output_thread = output.clone();
    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        while let Ok(count) = std::io::Read::read(&mut stdout, &mut buffer) {
            if count == 0 {
                break;
            }
            output_thread
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..count]);
        }
    });
    (child, stdin, output)
}

fn wait_for_line(
    output: &Arc<Mutex<Vec<u8>>>,
    from: usize,
    timeout: std::time::Duration,
) -> (Vec<u8>, usize) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let bytes = output.lock().unwrap();
        if let Some(end) = bytes[from..].iter().position(|&byte| byte == b'\n') {
            let end = from + end;
            return (bytes[from..end].to_vec(), end + 1);
        }
        drop(bytes);
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for protocol line"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn answer_action_and_read_cfg(
    stdin: &mut std::process::ChildStdin,
    output: &Arc<Mutex<Vec<u8>>>,
    from: usize,
) -> serde_json::Value {
    let action = serde_json::json!({
        "lang": "go",
        "version": "1.2.0",
        "confirm": true,
        "newline": "\n",
        "protocol": 4,
        "binary": true,
        "support_dir": true
    });
    writeln!(stdin, "#ACT:{}", escape::encode_string(&action.to_string())).unwrap();
    let (line, _) = wait_for_line(output, from, std::time::Duration::from_secs(3));
    let line = std::str::from_utf8(&line).unwrap();
    let encoded = line.strip_prefix("#CFG:").unwrap();
    serde_json::from_slice(&escape::decode_string(encoded).unwrap()).unwrap()
}

#[test]
fn trz_recursive_binary_cli_advertises_directory_and_latin1_escape_chars() {
    let destination = tempfile::tempdir().unwrap();
    let destination = destination.path().to_str().unwrap();
    let args = ["-q", "-r", "-b", "-e", destination];
    let (mut child, mut stdin, output) = spawn_rs_process("trz", &args);
    let (trigger, after_trigger) = wait_for_line(&output, 0, std::time::Duration::from_secs(3));
    assert!(String::from_utf8_lossy(&trigger).contains("TRANSFER:D:"));

    let config = answer_action_and_read_cfg(&mut stdin, &output, after_trigger);
    assert_eq!(config["directory"], true);
    assert_eq!(config["binary"], true);
    let chars = config["escape_chars"].as_array().unwrap();
    assert!(chars.iter().any(|pair| {
        pair[0]
            .as_str()
            .and_then(|value| value.chars().next())
            .is_some_and(|ch| ch as u32 == 0x8d)
    }));
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn tsz_recursive_cli_accepts_directory_and_advertises_directory_cfg() {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file.txt"), b"directory payload").unwrap();
    let source = source.path().to_str().unwrap();
    let args = ["-q", "-r", source];
    let (mut child, mut stdin, output) = spawn_rs_process("tsz", &args);
    let (trigger, after_trigger) = wait_for_line(&output, 0, std::time::Duration::from_secs(3));
    assert!(String::from_utf8_lossy(&trigger).contains("TRANSFER:S:"));

    let config = answer_action_and_read_cfg(&mut stdin, &output, after_trigger);
    assert_eq!(config["directory"], true);
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
#[test]
fn tmux_binary_modes_really_fall_back_to_base64() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let tmux = bin.join("tmux");
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut search_path = bin.as_os_str().to_os_string();
    search_path.push(":");
    search_path.push(path);
    let envs = [("TMUX", "fake-session".into()), ("PATH", search_path)];

    std::fs::write(&tmux, "#!/bin/sh\nprintf '%s\\n' '/dev/null:0:80'\n").unwrap();
    std::fs::set_permissions(&tmux, std::fs::Permissions::from_mode(0o755)).unwrap();
    let destination = tempfile::tempdir().unwrap();
    let destination_path = destination.path().to_str().unwrap().to_string();
    let (mut child, mut stdin, output) =
        spawn_rs_process_with_env("trz", &["-q", "-b", "-e", &destination_path], &envs);
    let (_, after_trigger) = wait_for_line(&output, 0, std::time::Duration::from_secs(3));
    let config = answer_action_and_read_cfg(&mut stdin, &output, after_trigger);
    assert!(config["binary"].is_null());
    assert_eq!(config["tmux_output_junk"], true);
    let _ = child.kill();
    let _ = child.wait();

    std::fs::write(&tmux, "#!/bin/sh\nprintf '%s\\n' ':1:80'\n").unwrap();
    let source = tempfile::tempdir().unwrap();
    let source_file = source.path().join("file.txt");
    std::fs::write(&source_file, b"payload").unwrap();
    let source_path = source_file.to_str().unwrap().to_string();
    let (mut child, mut stdin, output) =
        spawn_rs_process_with_env("tsz", &["-q", "-b", &source_path], &envs);
    let (_, after_trigger) = wait_for_line(&output, 0, std::time::Duration::from_secs(3));
    let config = answer_action_and_read_cfg(&mut stdin, &output, after_trigger);
    assert!(config["binary"].is_null());
    assert!(config["tmux_output_junk"].is_null());
    let _ = child.kill();
    let _ = child.wait();
}
