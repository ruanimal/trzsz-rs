use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use trzsz_rs::filter::{TrzszFilter, TrzszOptions};
use trzsz_rs::relay::TrzszRelay;

struct ChannelReader {
    receiver: Receiver<Vec<u8>>,
    current: Vec<u8>,
    offset: usize,
}

impl ChannelReader {
    fn new(receiver: Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            current: Vec::new(),
            offset: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            if self.offset < self.current.len() {
                let count = output.len().min(self.current.len() - self.offset);
                output[..count].copy_from_slice(&self.current[self.offset..self.offset + count]);
                self.offset += count;
                return Ok(count);
            }
            match self.receiver.recv() {
                Ok(bytes) => {
                    self.current = bytes;
                    self.offset = 0;
                }
                Err(_) => return Ok(0),
            }
        }
    }
}

struct ChannelWriter(Sender<Vec<u8>>);

impl Write for ChannelWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .send(bytes.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "relay pipe closed"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct IdleClientInput(Receiver<()>);

impl Read for IdleClientInput {
    fn read(&mut self, _output: &mut [u8]) -> io::Result<usize> {
        let _ = self.0.recv();
        Ok(0)
    }
}

fn build_go_trz(temp: &Path) -> PathBuf {
    let go_module = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("trzsz-go");
    let output = temp.join("go-trz");
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&output)
        .arg("./cmd/trz")
        .current_dir(go_module)
        .status()
        .expect("Go toolchain is required for relay interoperability tests");
    assert!(status.success(), "failed to build repository Go trz");
    output
}

fn wait_for_process(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll Go trz") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("Go trz transfer timed out");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn go_upload_completes_through_rust_filter_and_relay() {
    let temp = tempfile::tempdir().unwrap();
    let go_trz = build_go_trz(temp.path());
    let receive_dir = temp.path().join("received");
    std::fs::create_dir(&receive_dir).unwrap();
    let source = temp.path().join("source.bin");
    let expected = (0..256 * 1024 + 17)
        .map(|n| (n % 253) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&source, &expected).unwrap();

    let mut child = Command::new(go_trz)
        .args(["-q", "-y"])
        .arg(&receive_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start Go trz receiver");
    let go_stdin = child.stdin.take().unwrap();
    let go_stdout = child.stdout.take().unwrap();

    let (filter_to_relay_tx, filter_to_relay_rx) = mpsc::channel();
    let (relay_to_filter_tx, relay_to_filter_rx) = mpsc::channel();
    let relay = Arc::new(TrzszRelay::new(
        Box::new(ChannelReader::new(filter_to_relay_rx)),
        Box::new(ChannelWriter(relay_to_filter_tx)),
        Box::new(go_stdin),
        Box::new(go_stdout),
        TrzszOptions::default(),
    ));
    let relay_states = Arc::new(Mutex::new(Vec::new()));
    let state_capture = relay_states.clone();
    relay.set_transfer_state_callback(move |active| {
        state_capture.lock().unwrap().push(active);
    });

    let (client_wake_tx, client_wake_rx) = mpsc::channel();
    let filter = Arc::new(TrzszFilter::new(
        Box::new(IdleClientInput(client_wake_rx)),
        Box::new(io::sink()),
        Box::new(ChannelWriter(filter_to_relay_tx)),
        Box::new(ChannelReader::new(relay_to_filter_rx)),
        TrzszOptions::default(),
    ));
    filter.set_shutdown_handlers(
        Some(Arc::new(move || {
            let _ = client_wake_tx.send(());
        })),
        None,
    );
    let completion = filter
        .one_time_upload(std::slice::from_ref(&source))
        .unwrap();

    let relay_runner = relay.clone();
    let relay_thread = thread::spawn(move || relay_runner.run());
    let filter_runner = filter.clone();
    let filter_thread = thread::spawn(move || filter_runner.run());

    let status = wait_for_process(&mut child, Duration::from_secs(30));
    assert!(status.success(), "Go trz receiver failed: {status}");
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(())
    );
    assert_eq!(
        std::fs::read(receive_dir.join("source.bin")).unwrap(),
        expected
    );
    assert_eq!(&*relay_states.lock().unwrap(), &[true, false]);

    assert!(filter_thread.join().unwrap().is_ok());
    assert!(relay_thread.join().unwrap().is_ok());
}
