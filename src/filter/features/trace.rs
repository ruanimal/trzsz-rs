use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const ENABLE: &[u8] = b"<ENABLE_TRZSZ_TRACE_LOG>";
const DISABLE: &[u8] = b"<DISABLE_TRZSZ_TRACE_LOG>";
static TRACE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub(in crate::filter) struct TraceLogger {
    active: Option<(PathBuf, File)>,
    pending: Vec<u8>,
}

impl TraceLogger {
    pub fn log(&mut self, kind: &str, bytes: &[u8]) {
        if let Some((_, file)) = self.active.as_mut() {
            let _ = writeln!(file, "[{kind}]{}", encode_bytes(bytes));
        }
    }

    /// Replace trace control markers in remote output. Partial markers remain
    /// buffered so chunk boundaries cannot leak half-markers to the terminal.
    pub fn process_server_output(&mut self, bytes: &[u8]) -> Vec<u8> {
        self.pending.extend_from_slice(bytes);
        let mut output = Vec::new();
        loop {
            let enable = find_subslice(&self.pending, ENABLE);
            let disable = find_subslice(&self.pending, DISABLE);
            let marker = match (enable, disable) {
                (Some(left), Some(right)) if left <= right => Some((left, true, ENABLE.len())),
                (Some(_), Some(right)) => Some((right, false, DISABLE.len())),
                (Some(index), None) => Some((index, true, ENABLE.len())),
                (None, Some(index)) => Some((index, false, DISABLE.len())),
                (None, None) => None,
            };
            if let Some((index, enabling, length)) = marker {
                output.extend(self.pending.drain(..index));
                self.pending.drain(..length);
                if enabling {
                    if self.active.is_none() {
                        match create_trace_file() {
                            Ok((path, file)) => {
                                output.extend_from_slice(
                                    format!("Writing trace log to {}", path.display()).as_bytes(),
                                );
                                self.active = Some((path, file));
                            }
                            Err(error) => output.extend_from_slice(
                                format!("Create trace log error: {error}").as_bytes(),
                            ),
                        }
                    }
                } else if let Some((path, mut file)) = self.active.take() {
                    let _ = file.flush();
                    output.extend_from_slice(
                        format!("Closed trace log at {}", path.display()).as_bytes(),
                    );
                }
                continue;
            }
            let keep = longest_marker_prefix_suffix(&self.pending);
            let emit = self.pending.len().saturating_sub(keep);
            output.extend(self.pending.drain(..emit));
            return output;
        }
    }

    pub fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.pending)
    }
}

fn create_trace_file() -> std::io::Result<(PathBuf, File)> {
    let directory = std::env::temp_dir();
    for _ in 0..32 {
        let id = TRACE_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = directory.join(format!("trzsz_{}_{}.log", nanos, id));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "unable to allocate trace log path",
    ))
}

fn encode_bytes(bytes: &[u8]) -> String {
    crate::escape::encode_bytes(bytes)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn longest_marker_prefix_suffix(bytes: &[u8]) -> usize {
    [ENABLE, DISABLE]
        .iter()
        .map(|marker| {
            (1..marker.len().min(bytes.len() + 1))
                .rev()
                .find(|count| bytes.ends_with(&marker[..*count]))
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_markers_split_across_writes_are_replaced_and_open_close_logger() {
        let mut logger = TraceLogger::default();
        let out = logger.process_server_output(b"before<ENABLE_TRZSZ_");
        assert_eq!(out, b"before");
        let out = logger.process_server_output(b"TRACE_LOG>after");
        assert!(String::from_utf8_lossy(&out).contains("Writing trace log to"));
        assert!(logger.active.is_some());
        let path = logger.active.as_ref().unwrap().0.clone();
        let payload = b"data\x01";
        logger.log("stdin", payload);
        let out = logger.process_server_output(b"<DISABLE_TRZSZ_TRACE_LOG>done");
        assert!(String::from_utf8_lossy(&out).contains("Closed trace log at"));
        assert!(logger.active.is_none());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents.trim_end(),
            format!("[stdin]{}", crate::escape::encode_bytes(payload))
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn non_markers_are_preserved_including_partial_candidate() {
        let mut logger = TraceLogger::default();
        assert_eq!(logger.process_server_output(b"hello<ENABLE_TR"), b"hello");
        assert_eq!(logger.process_server_output(b"X"), b"<ENABLE_TRX");
    }
    #[test]
    fn trace_payload_uses_go_compatible_zlib_base64_encoding() {
        let encoded = encode_bytes(b"binary\0payload");
        assert_eq!(
            crate::escape::decode_string(&encoded).unwrap(),
            b"binary\0payload"
        );
    }
}
