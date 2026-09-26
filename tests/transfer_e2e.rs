//! End-to-end test: wire two TrzszTransfer instances together via in-memory
//! channels and verify a file transfer completes successfully.
//!
//! This bypasses the PTY/SSH layer entirely so we can test the protocol
//! implementation in isolation. If this hangs, the bug is purely inside
//! trzsz-rs itself.

use std::io::Write;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use trzsz_rs::comm::{CompressType, SourceFile};
use trzsz_rs::transfer::TrzszTransfer;

/// Writer that forwards every write into a SyncSender so the bytes land in
/// the peer transfer's input buffer.
struct ChanWriter {
    sender: SyncSender<Vec<u8>>,
}

impl Write for ChanWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sender
            .send(buf.to_vec())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn run_transfer_with_mode(test_data: Vec<u8>, binary: bool, compress: CompressType) {
    run_transfer_with_preexisting(test_data, binary, compress, None);
}

fn run_transfer_with_preexisting(
    test_data: Vec<u8>,
    binary: bool,
    compress: CompressType,
    preexisting: Option<Vec<u8>>,
) {
    // Set up source and destination paths.
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("source.txt");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir(&dst_dir).unwrap();
    if let Some(initial) = preexisting {
        std::fs::write(dst_dir.join("source.txt"), initial).unwrap();
    }
    std::fs::write(&src_path, &test_data).unwrap();

    let metadata = std::fs::metadata(&src_path).unwrap();
    let src_file = SourceFile {
        path_id: 0,
        abs_path: src_path.clone(),
        rel_path: vec!["source.txt".to_string()],
        is_dir: false,
        archive: false,
        size: metadata.len() as i64,
        perm: None,
    };

    // Create both transfers with throwaway writers, then cross-wire them.
    let mut transfer_a = TrzszTransfer::new(Box::new(std::io::sink()));
    let mut transfer_b = TrzszTransfer::new(Box::new(std::io::sink()));
    let a_sender = transfer_a.buffer.sender();
    let b_sender = transfer_b.buffer.sender();
    transfer_a.writer = Box::new(ChanWriter { sender: b_sender });
    transfer_b.writer = Box::new(ChanWriter { sender: a_sender });

    // Use a short timeout so a hang surfaces quickly instead of waiting 20s.
    transfer_a.transfer_config.timeout = 5;
    transfer_b.transfer_config.timeout = 5;

    let dst_dir_clone = dst_dir.clone();

    // B = server (tsz-like): recv_action -> send_config -> send_files -> recv_exit
    let b_handle = std::thread::spawn(move || -> Result<(), String> {
        let action = transfer_b
            .recv_action()
            .map_err(|e| format!("B recv_action: {}", e.message))?;

        let escape_value = if binary {
            serde_json::Value::Array(
                trzsz_rs::escape::get_escape_chars(true)
                    .iter()
                    .map(|(from, to)| {
                        serde_json::json!([
                            trzsz_rs::escape::bytes_to_latin1(from),
                            trzsz_rs::escape::bytes_to_latin1(to)
                        ])
                    })
                    .collect(),
            )
        } else {
            serde_json::Value::Null
        };
        transfer_b
            .send_config(
                true, // quiet
                binary,
                false, // directory
                true,  // overwrite
                &escape_value,
                0,
                &action,
                compress,
            )
            .map_err(|e| format!("B send_config: {}", e.message))?;

        transfer_b
            .send_files(&[src_file], &mut None)
            .map_err(|e| format!("B send_files: {}", e.message))?;

        transfer_b
            .recv_exit()
            .map_err(|e| format!("B recv_exit: {}", e.message))?;
        Ok(())
    });

    // A = client (filter download): send_action -> recv_config -> recv_files -> client_exit
    let a_handle = std::thread::spawn(move || -> Result<(), String> {
        transfer_a
            .send_action(true, None, false)
            .map_err(|e| format!("A send_action: {}", e.message))?;

        transfer_a
            .recv_config()
            .map_err(|e| format!("A recv_config: {}", e.message))?;

        transfer_a
            .recv_files(&dst_dir_clone, &mut None)
            .map_err(|e| format!("A recv_files: {}", e.message))?;

        transfer_a
            .client_exit("done")
            .map_err(|e| format!("A client_exit: {}", e.message))?;
        Ok(())
    });

    // Watchdog: fail fast on hang.
    let deadline = Instant::now() + Duration::from_secs(120);
    while !a_handle.is_finished() || !b_handle.is_finished() {
        if Instant::now() > deadline {
            panic!(
                "transfer hung: a_done={}, b_done={}",
                a_handle.is_finished(),
                b_handle.is_finished()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    a_handle.join().unwrap().expect("A side failed");
    b_handle.join().unwrap().expect("B side failed");

    let dst_path = dst_dir.join("source.txt");
    let received = std::fs::read(&dst_path).expect("read received file");
    assert_eq!(received.len(), test_data.len(), "size mismatch");
    assert_eq!(received, test_data, "content mismatch");
}

fn run_transfer_with_data(test_data: Vec<u8>) {
    run_transfer_with_mode(test_data, true, CompressType::No);
}

#[test]
fn test_v2_zstd_yes_and_no() {
    run_transfer_with_mode(vec![b'a'; 256 * 1024], false, CompressType::Yes);
    let mut data = Vec::with_capacity(256 * 1024);
    let mut state = 0x1234_5678_u32;
    for _ in 0..256 * 1024 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        data.push((state >> 24) as u8);
    }
    run_transfer_with_mode(data, false, CompressType::No);
}

#[test]
fn test_v2_auto_compression_boundaries_and_sampling() {
    run_transfer_with_mode(Vec::new(), false, CompressType::Auto);
    run_transfer_with_mode(vec![b'x'; 127], false, CompressType::Auto);
    run_transfer_with_mode(vec![b'x'; 1024], false, CompressType::Auto);

    let mut data = Vec::with_capacity(384 * 1024);
    let mut state = 0x89ab_cdef_u32;
    for _ in 0..384 * 1024 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        data.push((state >> 24) as u8);
    }
    run_transfer_with_mode(data, false, CompressType::Auto);
}

#[test]
fn test_v2_binary_zstd_and_escape() {
    let data = (0..128 * 1024)
        .map(|index| [0x1b, 0x03, 0x0d, 0x8d, 0xee, b'~'][index % 6])
        .collect();
    run_transfer_with_mode(data, true, CompressType::Yes);
}

#[test]
fn test_transfer_small_file() {
    run_transfer_with_data(b"Hello, trzsz!\n".to_vec());
}

#[test]
fn test_transfer_medium_file() {
    let mut data = Vec::with_capacity(4096);
    for i in 0..4096 {
        data.push((i % 256) as u8);
    }
    run_transfer_with_data(data);
}

#[test]
fn test_transfer_large_file() {
    let mut data = Vec::with_capacity(256 * 1024);
    for i in 0..(256 * 1024) {
        data.push(((i * 31) % 256) as u8);
    }
    run_transfer_with_data(data);
}

#[test]
fn test_v3_prefix_hash_resume_and_mismatch_fallback() {
    let complete = vec![b'c'; 256 * 1024];
    run_transfer_with_preexisting(complete.clone(), false, CompressType::No, Some(complete));

    let partial_data = vec![b'p'; 11 * 1024 * 1024 + 31];
    let matching_prefix = partial_data[..10 * 1024 * 1024].to_vec();
    run_transfer_with_preexisting(partial_data, false, CompressType::No, Some(matching_prefix));

    let mismatch_data = vec![b'm'; 21 * 1024 * 1024 + 17];
    let mut existing = mismatch_data[..20 * 1024 * 1024].to_vec();
    existing[10 * 1024 * 1024 + 7] ^= 0xff;
    run_transfer_with_preexisting(mismatch_data, false, CompressType::No, Some(existing));
}
