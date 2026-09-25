//! Interoperability test: trzsz-rs server (tsz) talking to a real
//! trzsz-go filter via PTY. Reproduces the user's "hang" scenario.
//!
//! This test is gated on:
//!   - the Go binaries being available at /tmp/go-trzsz
//!   - ~/.trzsz.conf containing DefaultDownloadPath = /tmp/trzsz_dl

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const DOWNLOAD_DIR: &str = "/tmp/trzsz_dl";

fn go_trzsz() -> Option<PathBuf> {
    let p = PathBuf::from("/tmp/go-trzsz");
    if p.exists() { Some(p) } else { None }
}

fn rs_tsz() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop();
    p.pop();
    p.push("tsz");
    if !p.exists() {
        panic!(
            "rs tsz binary not found at {}. Run `cargo build` first.",
            p.display()
        );
    }
    p
}

fn ensure_trzsz_conf() -> bool {
    let home = match std::env::var("HOME") {
        Ok(h) => h,
        Err(_) => return false,
    };
    let conf = Path::new(&home).join(".trzsz.conf");
    let want = format!("DefaultDownloadPath = {}", DOWNLOAD_DIR);
    let got = std::fs::read_to_string(&conf).unwrap_or_default();
    got.contains(&want)
}

#[test]
fn test_go_filter_downloads_from_rs_tsz() {
    let Some(go_trzsz) = go_trzsz() else {
        eprintln!("skipping: /tmp/go-trzsz not found");
        return;
    };
    if !ensure_trzsz_conf() {
        eprintln!(
            "skipping: ~/.trzsz.conf must contain `DefaultDownloadPath = {}`",
            DOWNLOAD_DIR
        );
        return;
    }
    let rs_tsz = rs_tsz();

    // Make sure the download dir exists and is empty for this run.
    let _ = std::fs::create_dir_all(DOWNLOAD_DIR);

    // Pick a unique source filename so we can verify it after.
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let filename = format!("rs_interop_{}.txt", stamp);

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join(&filename);
    let test_data = b"hello world from rs tsz interop test\n";
    std::fs::write(&src, test_data).unwrap();

    let dst_path = Path::new(DOWNLOAD_DIR).join(&filename);
    let _ = std::fs::remove_file(&dst_path);

    // Spawn go-trzsz wrapping bash inside a real PTY.
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("openpty");

    let mut cmd = CommandBuilder::new(&go_trzsz);
    cmd.arg("bash");
    cmd.cwd(dir.path());
    let mut child = pair.slave.spawn_command(cmd).expect("spawn go-trzsz");
    drop(pair.slave);

    let mut writer = pair.master.take_writer().unwrap();
    let mut reader = pair.master.try_clone_reader().unwrap();

    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let output_clone = output.clone();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = reader.read(&mut buf) {
            if n == 0 {
                break;
            }
            output_clone.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });

    // Give bash time to start.
    std::thread::sleep(Duration::from_millis(300));

    // Run rs-tsz inside the bash session.
    let cmdline = format!("{} -y -q {}\n", rs_tsz.display(), src.display());
    writer.write_all(cmdline.as_bytes()).unwrap();
    writer.flush().unwrap();

    // Wait for the file to land in DOWNLOAD_DIR.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut transfer_done = false;
    loop {
        if dst_path.exists() {
            // Wait briefly for the writer to flush.
            std::thread::sleep(Duration::from_millis(300));
            transfer_done = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Exit only after the transfer is done.
    let _ = writer.write_all(b"exit\n");
    let _ = writer.flush();
    drop(writer);

    let _ = child.kill();
    let _ = child.wait();

    let captured = output.lock().unwrap().clone();
    let captured_str = String::from_utf8_lossy(&captured);

    if !transfer_done {
        panic!(
            "transfer did not complete within 20s.\n--- PTY output ---\n{}\n--- end ---",
            captured_str
        );
    }

    let got = std::fs::read(&dst_path).expect("read received file");
    let _ = std::fs::remove_file(&dst_path);

    assert_eq!(
        got, test_data,
        "content mismatch.\nPTY output:\n{}",
        captured_str
    );
    eprintln!("PASS. PTY output:\n{}", captured_str);
}

/// Like the small-file test, but instead of just checking the file landed,
/// run rs-tsz **directly without a filter** with synthetic ACT/SUCC input
/// piped to its stdin and verify the process exits within a few seconds
/// after sending the file. Catches infinite loops in clean_input/server_exit.
#[test]
fn test_rs_tsz_exits_after_transfer() {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};

    let rs_tsz = rs_tsz();

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("exit_test.txt");
    std::fs::write(&src, b"exit me").unwrap();

    let mut child = Command::new(&rs_tsz)
        .args(["-y", "-q", src.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rs-tsz");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    // Reader thread for stdout.
    let stdout_buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let buf_clone = stdout_buf.clone();
    let stdout_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 {
                break;
            }
            buf_clone.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });

    fn find(buf: &[u8], needle: &[u8]) -> Option<usize> {
        if buf.len() < needle.len() {
            return None;
        }
        (0..=buf.len() - needle.len()).find(|&i| &buf[i..i + needle.len()] == needle)
    }

    fn read_line_at(
        buf: &std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
        from: usize,
        timeout: Duration,
    ) -> (Vec<u8>, usize) {
        let deadline = Instant::now() + timeout;
        loop {
            let b = buf.lock().unwrap();
            if b.len() > from {
                if let Some(rel) = find(&b[from..], b"\n") {
                    return (b[from..from + rel].to_vec(), from + rel + 1);
                }
            }
            drop(b);
            if Instant::now() > deadline {
                panic!("timeout waiting for line at offset {}", from);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn parse_typed(line: &[u8]) -> (String, String) {
        let s = String::from_utf8_lossy(line);
        let s = s.trim_start_matches(|c| c != '#');
        let stripped = s.strip_prefix('#').unwrap_or("");
        if let Some(idx) = stripped.find(':') {
            return (stripped[..idx].to_string(), stripped[idx + 1..].to_string());
        }
        (String::new(), String::new())
    }

    let mut pos: usize;

    // Wait for trigger.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if find(&stdout_buf.lock().unwrap(), b"::TRZSZ:TRANSFER:S:").is_some() {
            break;
        }
        if Instant::now() > deadline {
            panic!("trigger not received");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let (_trigger, np) = read_line_at(&stdout_buf, 0, Duration::from_secs(1));
    pos = np;

    // Send ACT.
    let action_json = serde_json::json!({
        "lang": "go", "version": "1.2.0", "confirm": true,
        "newline": "\n", "protocol": 1, "binary": true, "support_dir": true,
    })
    .to_string();
    let act = format!("#ACT:{}\n", trzsz_rs::escape::encode_string(&action_json));
    stdin.write_all(act.as_bytes()).unwrap();
    stdin.flush().unwrap();

    // CFG, NUM (+SUCC), NAME (+SUCC), SIZE (+SUCC), DATA (+SUCC), MD5 (+SUCC).
    let (_cfg, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3));
    pos = np;
    let (num, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3));
    pos = np;
    let (_, val) = parse_typed(&num);
    stdin
        .write_all(format!("#SUCC:{}\n", val).as_bytes())
        .unwrap();
    stdin.flush().unwrap();

    let (_name, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3));
    pos = np;
    let local = trzsz_rs::escape::encode_string("exit_test.txt");
    stdin
        .write_all(format!("#SUCC:{}\n", local).as_bytes())
        .unwrap();
    stdin.flush().unwrap();

    let (size_line, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3));
    pos = np;
    let (_, val) = parse_typed(&size_line);
    let size: usize = val.parse().unwrap();
    stdin
        .write_all(format!("#SUCC:{}\n", val).as_bytes())
        .unwrap();
    stdin.flush().unwrap();

    // DATA chunk(s).
    let mut received = 0usize;
    while received < size {
        let (data, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3));
        pos = np;
        let (_, val) = parse_typed(&data);
        let chunk = trzsz_rs::escape::decode_string(&val).expect("decode DATA");
        received += chunk.len();
        stdin
            .write_all(format!("#SUCC:{}\n", chunk.len()).as_bytes())
            .unwrap();
        stdin.flush().unwrap();
    }

    // MD5.
    let (md5_line, _np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3));
    let (_, val) = parse_typed(&md5_line);
    stdin
        .write_all(format!("#SUCC:{}\n", val).as_bytes())
        .unwrap();
    stdin.flush().unwrap();

    // Send EXIT.
    let exit_msg = trzsz_rs::escape::encode_string("done");
    stdin
        .write_all(format!("#EXIT:{}\n", exit_msg).as_bytes())
        .unwrap();
    stdin.flush().unwrap();

    // The process should exit promptly. Allow up to 3 seconds.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("rs-tsz did not exit within 3s after the protocol finished");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("wait error: {}", e),
        }
    }

    drop(stdin);
    drop(stdout_thread);
}
#[test]
fn test_go_filter_downloads_large_from_rs_tsz() {
    let Some(go_trzsz) = go_trzsz() else {
        eprintln!("skipping: /tmp/go-trzsz not found");
        return;
    };
    if !ensure_trzsz_conf() {
        eprintln!("skipping: ~/.trzsz.conf not configured");
        return;
    }
    let rs_tsz = rs_tsz();

    let _ = std::fs::create_dir_all(DOWNLOAD_DIR);

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let filename = format!("rs_interop_large_{}.bin", stamp);

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join(&filename);

    // 256 KB pseudo-random payload.
    let mut test_data = Vec::with_capacity(256 * 1024);
    for i in 0u32..(256 * 1024) {
        test_data.push(((i.wrapping_mul(31)) % 256) as u8);
    }
    std::fs::write(&src, &test_data).unwrap();

    let dst_path = Path::new(DOWNLOAD_DIR).join(&filename);
    let _ = std::fs::remove_file(&dst_path);

    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("openpty");

    let mut cmd = CommandBuilder::new(&go_trzsz);
    cmd.arg("bash");
    cmd.cwd(dir.path());
    let mut child = pair.slave.spawn_command(cmd).expect("spawn go-trzsz");
    drop(pair.slave);

    let mut writer = pair.master.take_writer().unwrap();
    let mut reader = pair.master.try_clone_reader().unwrap();

    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let output_clone = output.clone();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = reader.read(&mut buf) {
            if n == 0 {
                break;
            }
            output_clone.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });

    std::thread::sleep(Duration::from_millis(300));
    let cmdline = format!("{} -y -q {}\n", rs_tsz.display(), src.display());
    writer.write_all(cmdline.as_bytes()).unwrap();
    writer.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut transfer_done = false;
    loop {
        if dst_path.exists() {
            // Wait for size to settle.
            let prev_size = std::fs::metadata(&dst_path).map(|m| m.len()).unwrap_or(0);
            std::thread::sleep(Duration::from_millis(500));
            let new_size = std::fs::metadata(&dst_path).map(|m| m.len()).unwrap_or(0);
            if new_size == prev_size && new_size as usize == test_data.len() {
                transfer_done = true;
                break;
            }
        }
        if Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let _ = writer.write_all(b"exit\n");
    let _ = writer.flush();
    drop(writer);
    let _ = child.kill();
    let _ = child.wait();

    let captured = output.lock().unwrap().clone();
    let captured_str = String::from_utf8_lossy(&captured);

    if !transfer_done {
        let got_size = std::fs::metadata(&dst_path).map(|m| m.len()).unwrap_or(0);
        panic!(
            "large transfer did not complete within 60s. Got {}/{} bytes.\nPTY output:\n{}",
            got_size,
            test_data.len(),
            captured_str
        );
    }

    let got = std::fs::read(&dst_path).expect("read received file");
    let _ = std::fs::remove_file(&dst_path);
    assert_eq!(got.len(), test_data.len(), "size mismatch");
    assert_eq!(got, test_data, "content mismatch");
    eprintln!("PASS large ({} bytes).", got.len());
}
