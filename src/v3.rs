use std::io::SeekFrom;

use md5::Md5;
use sha2::Digest;

use crate::comm::{FileReader, FileWriter, TrzszError};
use crate::progress::ProgressCallback;
use crate::transfer::TrzszTransfer;

const PREFIX_HASH_STEP: i64 = 10 * 1024 * 1024;
const PREFIX_HASH_BUFFER_SIZE: usize = 64 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
struct PrefixHash {
    #[serde(default)]
    step: i64,
    #[serde(default)]
    hash: String,
    #[serde(default)]
    over: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PrefixHashAck {
    step: i64,
    #[serde(rename = "match")]
    matched: bool,
}

pub(crate) fn send_prefix_hash(
    transfer: &mut TrzszTransfer,
    file: &mut dyn FileReader,
    source_size: i64,
    target_size: i64,
    progress: &mut Option<&mut dyn ProgressCallback>,
) -> Result<i64, TrzszError> {
    if source_size < 0 || target_size < 0 {
        return Err(crate::comm::simple_error("Invalid file size for HASH"));
    }
    if let Some(callback) = progress.as_mut() {
        callback.on_size(source_size);
    }

    transfer.send_integer("SIZE", source_size)?;

    let prefix_size = source_size.min(target_size);
    file.seek(SeekFrom::Start(0))
        .map_err(|e| crate::comm::simple_trzsz_error("Seek source file for HASH failed", e))?;

    let num_checkpoints =
        prefix_size / PREFIX_HASH_STEP + i64::from(prefix_size % PREFIX_HASH_STEP != 0);
    let mut hasher = Md5::new();
    let mut buffer = vec![0_u8; PREFIX_HASH_BUFFER_SIZE];
    let mut step = 0_i64;
    while step < prefix_size {
        let next_step = step.saturating_add(PREFIX_HASH_STEP).min(prefix_size);
        while step < next_step {
            let count = usize::try_from((next_step - step).min(buffer.len() as i64))
                .map_err(|e| crate::comm::simple_trzsz_error("Invalid HASH step", e))?;
            read_source_exact(file, &mut buffer[..count])?;
            hasher.update(&buffer[..count]);
            step += count as i64;
        }
        let digest = hasher.clone().finalize();
        let hash = PrefixHash {
            step,
            hash: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
            over: false,
        };
        let payload = serde_json::to_string(&hash)
            .map_err(|e| crate::comm::simple_trzsz_error("Marshal HASH failed", e))?;
        transfer.send_string("HASH", &payload)?;
    }
    transfer.send_string(
        "HASH",
        &serde_json::to_string(&PrefixHash {
            step: 0,
            hash: String::new(),
            over: true,
        })
        .map_err(|e| crate::comm::simple_trzsz_error("Marshal HASH end marker failed", e))?,
    )?;

    let mut match_step = 0;
    let mut expected_step = 0_i64;
    for _ in 0..num_checkpoints {
        expected_step = expected_step
            .saturating_add(PREFIX_HASH_STEP)
            .min(prefix_size);
        let payload = transfer.recv_string("SUCC", false, transfer.get_new_timeout())?;
        let ack: PrefixHashAck = serde_json::from_str(&payload)
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid HASH acknowledgement", e))?;
        if ack.step != expected_step {
            return Err(crate::comm::simple_trzsz_error(
                "Invalid HASH acknowledgement step",
                format!("{} <> {}", ack.step, expected_step),
            ));
        }
        if !ack.matched {
            break;
        }
        match_step = ack.step;
        if let Some(callback) = progress.as_mut() {
            callback.on_step(match_step);
        }
    }

    file.seek(SeekFrom::Start(match_step as u64))
        .map_err(|e| crate::comm::simple_trzsz_error("Seek source file after HASH failed", e))?;
    if let Some(callback) = progress.as_mut() {
        callback.set_pre_size(match_step);
    }
    Ok(source_size - match_step)
}

pub(crate) fn recv_prefix_hash(
    transfer: &mut TrzszTransfer,
    file: &mut dyn FileWriter,
    target_size: i64,
    progress: &mut Option<&mut dyn ProgressCallback>,
) -> Result<i64, TrzszError> {
    if target_size < 0 {
        return Err(crate::comm::simple_error("Invalid target file size"));
    }
    let local_size =
        i64::try_from(file.size().map_err(|e| {
            crate::comm::simple_trzsz_error("Get target file size for HASH failed", e)
        })?)
        .map_err(|e| crate::comm::simple_trzsz_error("Invalid target file size", e))?;
    if let Some(callback) = progress.as_mut() {
        callback.on_size(target_size);
    }

    let source_size = transfer.recv_integer("SIZE", false, transfer.get_new_timeout())?;
    if source_size < 0 {
        return Err(crate::comm::simple_error("Invalid file size for HASH"));
    }

    let prefix_size = source_size.min(local_size);
    file.seek(SeekFrom::Start(0))
        .map_err(|e| crate::comm::simple_trzsz_error("Seek target file for HASH failed", e))?;
    let mut hasher = Md5::new();
    let mut buffer = vec![0_u8; PREFIX_HASH_BUFFER_SIZE];
    let mut step = 0_i64;
    let mut match_step = 0_i64;
    let mut matched = true;
    loop {
        let payload = transfer.recv_string("HASH", false, transfer.get_new_timeout())?;
        let hash: PrefixHash = serde_json::from_str(&payload)
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid HASH message", e))?;
        if hash.over {
            if hash.step != 0 || !hash.hash.is_empty() || step != prefix_size {
                return Err(crate::comm::simple_error("Invalid HASH end marker"));
            }
            break;
        }

        let expected_step = step.saturating_add(PREFIX_HASH_STEP).min(prefix_size);
        if expected_step <= step || hash.step != expected_step || !is_md5_hex(&hash.hash) {
            return Err(crate::comm::simple_trzsz_error(
                "Invalid HASH step or digest",
                hash.step,
            ));
        }
        let next_step = hash.step;
        if matched {
            while step < next_step {
                let count = usize::try_from((next_step - step).min(buffer.len() as i64))
                    .map_err(|e| crate::comm::simple_trzsz_error("Invalid HASH step", e))?;
                read_target_exact(file, &mut buffer[..count])?;
                hasher.update(&buffer[..count]);
                step += count as i64;
            }
            matched = digest_matches(&hasher, &hash.hash);
            if matched {
                match_step = hash.step;
                if let Some(callback) = progress.as_mut() {
                    callback.on_step(match_step);
                }
            }
            let ack = PrefixHashAck {
                step: hash.step,
                matched,
            };
            let payload = serde_json::to_string(&ack).map_err(|e| {
                crate::comm::simple_trzsz_error("Marshal HASH acknowledgement failed", e)
            })?;
            transfer.send_string("SUCC", &payload)?;
        } else {
            step = hash.step;
        }
    }

    file.seek(SeekFrom::Start(match_step as u64))
        .map_err(|e| crate::comm::simple_trzsz_error("Seek target file after HASH failed", e))?;
    file.set_len(match_step as u64).map_err(|e| {
        crate::comm::simple_trzsz_error("Truncate target file after HASH failed", e)
    })?;
    if let Some(callback) = progress.as_mut() {
        callback.set_pre_size(match_step);
    }
    Ok(source_size - match_step)
}

fn read_source_exact(file: &mut dyn FileReader, mut buf: &mut [u8]) -> Result<(), TrzszError> {
    while !buf.is_empty() {
        let count = file.read(buf).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        if count == 0 {
            return Err(crate::comm::simple_error(
                "Unexpected EOF while hashing source file",
            ));
        }
        buf = &mut buf[count..];
    }
    Ok(())
}

fn read_target_exact(file: &mut dyn FileWriter, mut buf: &mut [u8]) -> Result<(), TrzszError> {
    while !buf.is_empty() {
        let count = file.read(buf).map_err(|e| {
            crate::comm::simple_trzsz_error("Read target prefix for HASH failed", e)
        })?;
        if count == 0 {
            return Err(crate::comm::simple_error(
                "Truncated target prefix for HASH",
            ));
        }
        buf = &mut buf[count..];
    }
    Ok(())
}

fn digest_matches(hasher: &Md5, expected: &str) -> bool {
    let digest = hasher.clone().finalize();
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        .eq_ignore_ascii_case(expected)
}

fn is_md5_hex(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor, Write};

    use crate::escape;

    struct MemoryFile {
        bytes: Cursor<Vec<u8>>,
        reported_size: Option<u64>,
    }

    impl MemoryFile {
        fn new(bytes: Vec<u8>) -> Self {
            MemoryFile {
                bytes: Cursor::new(bytes),
                reported_size: None,
            }
        }
    }

    impl FileWriter for MemoryFile {
        fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
            self.bytes.write_all(data)
        }

        fn close(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
            io::Read::read(&mut self.bytes, data)
        }

        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            io::Seek::seek(&mut self.bytes, position)
        }

        fn size(&self) -> io::Result<u64> {
            Ok(self
                .reported_size
                .unwrap_or_else(|| self.bytes.get_ref().len() as u64))
        }

        fn set_len(&mut self, size: u64) -> io::Result<()> {
            self.bytes.get_mut().truncate(size as usize);
            Ok(())
        }
    }

    struct MemoryReader {
        bytes: Cursor<Vec<u8>>,
    }

    impl FileReader for MemoryReader {
        fn read(&mut self, data: &mut [u8]) -> io::Result<usize> {
            io::Read::read(&mut self.bytes, data)
        }

        fn size(&self) -> i64 {
            self.bytes.get_ref().len() as i64
        }

        fn close(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            io::Seek::seek(&mut self.bytes, position)
        }
    }

    fn test_transfer(wire: &[u8]) -> TrzszTransfer {
        let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
        transfer.transfer_config.timeout = 2;
        transfer.add_received_data(wire, false);
        transfer
    }

    fn hash_wire(size: i64, records: &[&str]) -> Vec<u8> {
        let mut wire = format!("#SIZE:{size}\n").into_bytes();
        for record in records {
            wire.extend_from_slice(format!("#HASH:{}\n", escape::encode_string(record)).as_bytes());
        }
        wire
    }

    #[test]
    fn receive_prefix_hash_truncates_after_last_matching_checkpoint() {
        let prefix = b"matching prefix";
        let mut hasher = Md5::new();
        hasher.update(prefix);
        let digest = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let records = [
            serde_json::json!({"step": prefix.len(), "hash": digest}).to_string(),
            serde_json::json!({"over": true}).to_string(),
        ];
        let wire = hash_wire(prefix.len() as i64 + 5, &[&records[0], &records[1]]);
        let mut transfer = test_transfer(&wire);
        let mut file = MemoryFile::new(prefix.to_vec());
        let mut progress = None;

        let remaining = recv_prefix_hash(
            &mut transfer,
            &mut file,
            prefix.len() as i64 + 5,
            &mut progress,
        )
        .unwrap();

        assert_eq!(remaining, 5);
        assert_eq!(file.bytes.get_ref(), prefix);
        assert_eq!(file.bytes.position(), prefix.len() as u64);
    }

    #[test]
    fn malformed_hash_step_and_digest_are_rejected() {
        for record in [
            r#"{"step":0,"hash":"00000000000000000000000000000000"}"#,
            r#"{"step":2,"hash":"bad"}"#,
            r#"{"step":4,"hash":"00000000000000000000000000000000"}"#,
        ] {
            let records = [record, r#"{"over":true}"#];
            let wire = hash_wire(3, &records);
            let mut transfer = test_transfer(&wire);
            let mut file = MemoryFile::new(b"abc".to_vec());
            let error = recv_prefix_hash(&mut transfer, &mut file, 3, &mut None).unwrap_err();
            assert!(error.message.contains("Invalid HASH"));
        }
    }

    #[test]
    fn truncated_existing_prefix_is_rejected() {
        let record = r#"{"step":3,"hash":"900150983cd24fb0d6963f7d28e17f72"}"#;
        let wire = hash_wire(4, &[record, r#"{"over":true}"#]);
        let mut transfer = test_transfer(&wire);
        let mut file = MemoryFile::new(b"ab".to_vec());
        file.reported_size = Some(3);
        let error = recv_prefix_hash(&mut transfer, &mut file, 4, &mut None).unwrap_err();
        assert!(error.message.contains("Truncated target prefix"));
    }

    #[test]
    fn malformed_hash_payload_is_rejected() {
        let wire = hash_wire(1, &["not json"]);
        let mut transfer = test_transfer(&wire);
        let mut file = MemoryFile::new(b"a".to_vec());
        let error = recv_prefix_hash(&mut transfer, &mut file, 1, &mut None).unwrap_err();
        assert!(error.message.contains("Invalid HASH message"));
    }

    #[test]
    fn malformed_hash_ack_payload_and_step_are_rejected() {
        for ack in ["not json", r#"{"step":4,"match":true}"#] {
            let wire = format!("#SUCC:{}\n", escape::encode_string(ack));
            let mut transfer = test_transfer(wire.as_bytes());
            let mut file = MemoryReader {
                bytes: Cursor::new(b"abc".to_vec()),
            };
            let error = send_prefix_hash(&mut transfer, &mut file, 3, 3, &mut None).unwrap_err();
            assert!(error.message.contains("HASH acknowledgement"));
        }
    }
}
