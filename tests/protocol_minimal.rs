//! Drive a real `tsz` process end-to-end, simulating the Go filter side of
//! the protocol. Pinpoints exactly where the transfer hangs.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn rs_tsz() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop();
    p.pop();
    p.push("tsz");
    if !p.exists() {
        panic!("rs tsz not found at {}", p.display());
    }
    p
}

fn find_subslice(buf: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || buf.len() < needle.len() {
        return None;
    }
    for i in 0..=(buf.len() - needle.len()) {
        if &buf[i..i + needle.len()] == needle {
            return Some(i);
        }
    }
    None
}

/// Read from the buffer until we find a `\n`, starting from `from`.
/// Returns (line_without_newline, end_pos_after_newline).
fn read_line_at(
    stdout_buf: &Arc<Mutex<Vec<u8>>>,
    from: usize,
    timeout: Duration,
) -> Option<(Vec<u8>, usize)> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let buf = stdout_buf.lock().unwrap();
        if buf.len() > from {
            if let Some(rel) = find_subslice(&buf[from..], b"\n") {
                let line = buf[from..from + rel].to_vec();
                return Some((line, from + rel + 1));
            }
        }
        drop(buf);
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

/// Read exactly `n` bytes starting from `from`. Returns end position.
#[allow(dead_code)]
fn read_n_at(
    stdout_buf: &Arc<Mutex<Vec<u8>>>,
    from: usize,
    n: usize,
    timeout: Duration,
) -> Option<(Vec<u8>, usize)> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let buf = stdout_buf.lock().unwrap();
        if buf.len() >= from + n {
            return Some((buf[from..from + n].to_vec(), from + n));
        }
        drop(buf);
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

/// Parse a `#TYPE:value` line.
fn parse_typed_line(line: &[u8]) -> (String, String) {
    let s = String::from_utf8_lossy(line);
    let s = s.trim_start_matches(|c| c != '#');
    if let Some(stripped) = s.strip_prefix('#') {
        if let Some(idx) = stripped.find(':') {
            let typ = &stripped[..idx];
            let val = &stripped[idx + 1..];
            return (typ.to_string(), val.to_string());
        }
    }
    (String::new(), String::new())
}

#[test]
fn test_full_handshake_and_data_transfer() {
    let rs_tsz = rs_tsz();

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("hello.txt");
    let test_data = b"hello world from rs tsz\n";
    std::fs::write(&src, test_data).unwrap();

    let mut child = Command::new(&rs_tsz)
        .args(["-y", "-q", src.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rs-tsz");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let stdout_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let stdout_buf_clone = stdout_buf.clone();
    let stdout_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 { break; }
            stdout_buf_clone.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });

    let mut pos: usize;

    // 1. Wait for trigger ::TRZSZ:TRANSFER:S:...
    let deadline = Instant::now() + Duration::from_secs(3);
    let trigger_pos = loop {
        if let Some(p) = find_subslice(&stdout_buf.lock().unwrap(), b"::TRZSZ:TRANSFER:S:") {
            break p;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("trigger not received");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    eprintln!("step 1: got trigger at pos {}", trigger_pos);
    // Skip past trigger line
    let (_trigger_line, np) = read_line_at(&stdout_buf, trigger_pos, Duration::from_secs(1))
        .expect("trigger line");
    pos = np;

    // 2. Send ACT
    let action_json = serde_json::json!({
        "lang": "go", "version": "1.2.0", "confirm": true,
        "newline": "\n", "protocol": 4, "binary": true, "support_dir": true,
    }).to_string();
    let act_line = format!("#ACT:{}\n", trzsz_rs::escape::encode_string(&action_json));
    stdin.write_all(act_line.as_bytes()).unwrap();
    stdin.flush().unwrap();
    eprintln!("step 2: sent ACT");

    // 3. Receive CFG
    let (cfg_line, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3))
        .expect("CFG line");
    let (typ, val) = parse_typed_line(&cfg_line);
    assert_eq!(typ, "CFG", "expected CFG, got {:?}", String::from_utf8_lossy(&cfg_line));
    pos = np;
    let cfg_decoded = trzsz_rs::escape::decode_string(&val).expect("decode cfg");
    let cfg_str = String::from_utf8_lossy(&cfg_decoded);
    eprintln!("step 3: got CFG: {}", cfg_str);

    // 4. Receive NUM (number of files)
    let (num_line, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3))
        .expect("NUM line");
    let (typ, val) = parse_typed_line(&num_line);
    assert_eq!(typ, "NUM");
    let num: i64 = val.parse().unwrap();
    eprintln!("step 4: got NUM={}", num);
    pos = np;

    // Send SUCC for NUM
    stdin.write_all(format!("#SUCC:{}\n", num).as_bytes()).unwrap();
    stdin.flush().unwrap();
    eprintln!("step 4: sent SUCC for NUM");

    // 5. Receive NAME
    let (name_line, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3))
        .expect("NAME line");
    let (typ, _val) = parse_typed_line(&name_line);
    assert_eq!(typ, "NAME");
    pos = np;
    eprintln!("step 5: got NAME");

    // Send SUCC with the local name (encoded string).
    let local_name_encoded = trzsz_rs::escape::encode_string("hello.txt");
    stdin.write_all(format!("#SUCC:{}\n", local_name_encoded).as_bytes()).unwrap();
    stdin.flush().unwrap();
    eprintln!("step 5: sent SUCC for NAME");

    // 6. Receive SIZE
    let (size_line, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3))
        .expect("SIZE line");
    let (typ, val) = parse_typed_line(&size_line);
    assert_eq!(typ, "SIZE");
    let size: i64 = val.parse().unwrap();
    eprintln!("step 6: got SIZE={}", size);
    pos = np;

    stdin.write_all(format!("#SUCC:{}\n", size).as_bytes()).unwrap();
    stdin.flush().unwrap();
    eprintln!("step 6: sent SUCC for SIZE");

    // 7. Receive DATA chunks. CFG didn't say "binary", so this is base64 mode:
    // each line is "#DATA:<base64(zlib(payload))>\n" and the decoded length
    // is the actual payload length.
    let mut received_data = Vec::new();
    while (received_data.len() as i64) < size {
        let (data_hdr, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(5))
            .unwrap_or_else(|| {
                let snap = stdout_buf.lock().unwrap().clone();
                let _ = child.kill();
                panic!(
                    "DATA header not received. pos={} buf_len={}, recent: {:?}",
                    pos, snap.len(),
                    String::from_utf8_lossy(&snap[snap.len().saturating_sub(200)..])
                );
            });
        let (typ, val) = parse_typed_line(&data_hdr);
        assert_eq!(typ, "DATA", "expected DATA, got {:?}", String::from_utf8_lossy(&data_hdr));
        // base64 mode: val is base64(zlib(payload))
        let chunk = trzsz_rs::escape::decode_string(&val).expect("decode DATA payload");
        let chunk_size = chunk.len();
        received_data.extend_from_slice(&chunk);
        pos = np;
        eprintln!("step 7: got DATA chunk ({} bytes), total {}/{}", chunk_size, received_data.len(), size);

        // Send SUCC ack with the chunk size (V1 ack format)
        stdin.write_all(format!("#SUCC:{}\n", chunk_size).as_bytes()).unwrap();
        stdin.flush().unwrap();
    }

    eprintln!("step 7: all DATA received: {} bytes", received_data.len());

    // 8. Receive MD5
    let (md5_line, np) = read_line_at(&stdout_buf, pos, Duration::from_secs(3))
        .expect("MD5 line");
    let (typ, val) = parse_typed_line(&md5_line);
    assert_eq!(typ, "MD5");
    let _ = np;
    eprintln!("step 8: got MD5");

    // Send SUCC with same digest
    stdin.write_all(format!("#SUCC:{}\n", val).as_bytes()).unwrap();
    stdin.flush().unwrap();

    // 9. Send EXIT
    let exit_msg = trzsz_rs::escape::encode_string("done");
    stdin.write_all(format!("#EXIT:{}\n", exit_msg).as_bytes()).unwrap();
    stdin.flush().unwrap();
    eprintln!("step 9: sent EXIT");

    // Verify content
    assert_eq!(received_data, test_data);
    eprintln!("PASS: data matches");

    // Cleanup. Drop stdin to signal EOF, kill child to be safe, and detach
    // the stdout thread without joining (it will exit when the pipe closes).
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    drop(stdout_thread);
}
