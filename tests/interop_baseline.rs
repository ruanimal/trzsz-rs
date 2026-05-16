//! Baseline: same setup as interop.rs, but using go-tsz on the server side
//! to verify the filter and config are working in this environment.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};

const DOWNLOAD_DIR: &str = "/tmp/trzsz_dl";

fn go_bin(name: &str) -> Option<PathBuf> {
    let p = PathBuf::from(format!("/tmp/{}", name));
    if p.exists() { Some(p) } else { None }
}

fn ensure_trzsz_conf() -> bool {
    let home = std::env::var("HOME").ok().unwrap_or_default();
    let conf = Path::new(&home).join(".trzsz.conf");
    let want = format!("DefaultDownloadPath = {}", DOWNLOAD_DIR);
    let got = std::fs::read_to_string(&conf).unwrap_or_default();
    got.contains(&want)
}

#[test]
fn test_go_filter_downloads_from_go_tsz() {
    let Some(go_trzsz) = go_bin("go-trzsz") else {
        eprintln!("skipping: /tmp/go-trzsz not found");
        return;
    };
    let Some(go_tsz) = go_bin("go-tsz") else {
        eprintln!("skipping: /tmp/go-tsz not found");
        return;
    };
    if !ensure_trzsz_conf() {
        eprintln!("skipping: ~/.trzsz.conf not configured");
        return;
    }

    let _ = std::fs::create_dir_all(DOWNLOAD_DIR);

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let filename = format!("go_baseline_{}.txt", stamp);

    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join(&filename);
    let test_data = b"baseline data\n";
    std::fs::write(&src, test_data).unwrap();

    let dst_path = Path::new(DOWNLOAD_DIR).join(&filename);
    let _ = std::fs::remove_file(&dst_path);

    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize { rows: 24, cols: 80, pixel_width: 0, pixel_height: 0 })
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
            if n == 0 { break; }
            output_clone.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });

    std::thread::sleep(Duration::from_millis(300));

    // Run go-tsz inside the bash session. Don't send "exit" yet - that would
    // be routed to go-tsz by the terminal driver since it's the foreground
    // process. We'll exit cleanly after the transfer completes.
    let cmdline = format!("{} -y -q {}\n", go_tsz.display(), src.display());
    writer.write_all(cmdline.as_bytes()).unwrap();
    writer.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut transfer_done = false;
    loop {
        if dst_path.exists() {
            std::thread::sleep(Duration::from_millis(300));
            transfer_done = true;
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Send exit only after transfer is done (or timed out).
    let _ = writer.write_all(b"exit\n");
    let _ = writer.flush();
    drop(writer);

    let _ = child.kill();
    let _ = child.wait();

    let captured = output.lock().unwrap().clone();
    let captured_str = String::from_utf8_lossy(&captured);

    if !transfer_done {
        panic!(
            "go-tsz baseline transfer did not complete within 20s.\n\
             --- PTY output ---\n{}\n--- end ---",
            captured_str
        );
    }

    let got = std::fs::read(&dst_path).unwrap();
    let _ = std::fs::remove_file(&dst_path);
    assert_eq!(got, test_data, "PTY output:\n{}", captured_str);
    eprintln!("Baseline OK. PTY output:\n{}", captured_str);
}
