use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

type PtyChild = Box<dyn portable_pty::Child + Send + Sync>;

fn build_go_filter(temp: &Path) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let go_module = root.join("trzsz-go");
    assert!(
        go_module.join("go.mod").is_file(),
        "Go reference module missing"
    );
    let output = temp.join("go-trzsz-v2");
    let status = std::process::Command::new("go")
        .args(["build", "-o"])
        .arg(&output)
        .args(["./cmd/trzsz"])
        .current_dir(go_module)
        .status()
        .expect("Go toolchain is required for V2 interoperability tests");
    assert!(status.success(), "failed to build repository Go filter");
    output
}
fn build_go_upload_probe(temp: &Path) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let go_module = root.join("trzsz-go");
    let source = temp.join("go_upload_probe.go");
    let output = temp.join("go-upload-probe");
    std::fs::write(
        &source,
        r#"package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/trzsz/trzsz-go/trzsz"
)

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 4 {
		fail(fmt.Errorf("usage: probe upload-file trz-bin destination"))
	}
	server := exec.Command(os.Args[2], "-y", "-q", os.Args[3])
	serverIn, err := server.StdinPipe()
	if err != nil { fail(err) }
	serverOut, err := server.StdoutPipe()
	if err != nil { fail(err) }
	server.Stderr = os.Stderr
	if err := server.Start(); err != nil { fail(err) }
	clientInput, clientInputWriter := io.Pipe()
	filter := trzsz.NewTrzszFilter(clientInput, os.Stdout, serverIn, serverOut,
		trzsz.TrzszOptions{TerminalColumns: 80})
	result, err := filter.OneTimeUpload([]string{os.Args[1]})
	if err != nil {
		filter.Close()
		_ = server.Process.Kill()
		_ = server.Wait()
		fail(err)
	}
	if err := <-result; err != nil {
		filter.Close()
		_ = server.Process.Kill()
		_ = server.Wait()
		fail(err)
	}
	_ = clientInputWriter.Close()
	filter.Close()
	if err := server.Wait(); err != nil { fail(err) }
}
"#,
    )
    .expect("write Go upload probe");
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&output)
        .arg(&source)
        .current_dir(go_module)
        .status()
        .expect("Go toolchain is required for V2 interoperability tests");
    assert!(status.success(), "failed to build Go OneTimeUpload probe");
    output
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn start_go_filter(
    go_binary: &Path,
    home: &Path,
    cwd: &Path,
) -> (
    PtyChild,
    Box<dyn Write + Send>,
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
) {
    let pty = native_pty_system();
    let pair = pty
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open Go filter PTY");
    let mut command = CommandBuilder::new(go_binary);
    command.arg("bash");
    command.cwd(cwd);
    command.env("HOME", home);
    let child = pair.slave.spawn_command(command).expect("spawn Go filter");
    drop(pair.slave);

    let writer = pair.master.take_writer().expect("PTY writer");
    let mut reader = pair.master.try_clone_reader().expect("PTY reader");
    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let captured = output.clone();
    thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        while let Ok(count) = reader.read(&mut buffer) {
            if count == 0 {
                break;
            }
            captured.lock().unwrap().extend_from_slice(&buffer[..count]);
        }
    });
    (child, writer, output)
}

fn wait_for_file(
    path: &Path,
    expected: &[u8],
    timeout: Duration,
    output: &std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(actual) = std::fs::read(path) {
            if actual == expected {
                return;
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "V2 Go interoperability transfer timed out for {}. PTY output:\n{}",
                path.display(),
                String::from_utf8_lossy(&output.lock().unwrap())
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn stop_filter(mut child: PtyChild, mut writer: Box<dyn Write + Send>) {
    let _ = writer.write_all(b"exit\n");
    let _ = writer.flush();
    drop(writer);
    let _ = child.kill();
    let _ = child.wait();
}

fn create_home(root: &Path, key: &str, value: &Path) -> PathBuf {
    let home = root.join("home");
    let _ = std::fs::create_dir_all(&home);
    std::fs::write(
        home.join(".trzsz.conf"),
        format!("{} = {}\n", key, value.display()),
    )
    .expect("write isolated Go filter config");
    home
}

#[test]
fn go_filter_downloads_v2_zstd_stream_from_rust_tsz() {
    let temp = tempfile::tempdir().unwrap();
    let go_binary = build_go_filter(temp.path());
    let download = temp.path().join("downloads");
    std::fs::create_dir_all(&download).unwrap();
    let home = create_home(temp.path(), "DefaultDownloadPath", &download);
    let source = temp.path().join("compressed-source.bin");
    let expected = vec![b'v'; 512 * 1024];
    std::fs::write(&source, &expected).unwrap();

    let (child, mut writer, output) = start_go_filter(&go_binary, &home, temp.path());
    thread::sleep(Duration::from_millis(300));
    let command = format!(
        "{} -y -q -c auto {}\n",
        shell_quote(Path::new(env!("CARGO_BIN_EXE_tsz"))),
        shell_quote(&source)
    );
    writer.write_all(command.as_bytes()).unwrap();
    writer.flush().unwrap();

    let received = download.join("compressed-source.bin");
    wait_for_file(&received, &expected, Duration::from_secs(30), &output);
    stop_filter(child, writer);
}

#[test]
fn go_filter_uploads_v2_zstd_stream_to_rust_trz() {
    let temp = tempfile::tempdir().unwrap();
    let go_probe = build_go_upload_probe(temp.path());
    let upload = temp.path().join("go-source.bin");
    let destination = temp.path().join("received");
    std::fs::create_dir_all(&destination).unwrap();
    let expected = vec![b'g'; 384 * 1024];
    std::fs::write(&upload, &expected).unwrap();

    let status = Command::new(go_probe)
        .arg(&upload)
        .arg(env!("CARGO_BIN_EXE_trz"))
        .arg(&destination)
        .status()
        .expect("run Go OneTimeUpload probe");
    assert!(status.success(), "Go OneTimeUpload probe failed");
    assert_eq!(
        std::fs::read(destination.join("go-source.bin")).unwrap(),
        expected
    );
}
