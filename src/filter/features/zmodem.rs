use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const MAX_DETECT_BUFFER: usize = 4096;
const INIT_PREFIX: &[u8] = b"**\x18B0";
const FINISH_PREFIX: &[u8] = b"**\x18B08";
pub(in crate::filter) const OVER_AND_OUT: &[u8] = b"OO\x08\x08";
pub(in crate::filter) const CANCEL_SEQUENCE: &[u8] =
    b"\x18\x18\x18\x18\x18\x18\x18\x18\x18\x18\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08";
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::filter) struct ZmodemInit {
    /// The remote endpoint is asking the local endpoint to send files.
    pub upload: bool,
}

#[derive(Default)]
pub(in crate::filter) struct ZmodemDetector {
    pending: Vec<u8>,
}

impl ZmodemDetector {
    /// Return ordinary bytes and, when found, a ZMODEM initiation plus bytes
    /// following it. Fragmented signatures are retained with a strict bound.
    pub fn push(&mut self, bytes: &[u8]) -> (Vec<u8>, Option<(ZmodemInit, Vec<u8>)>) {
        self.pending.extend_from_slice(bytes);
        let mut ordinary = Vec::new();
        loop {
            let Some(index) = find_subslice(&self.pending, INIT_PREFIX) else {
                let keep = (1..=INIT_PREFIX.len().min(self.pending.len()))
                    .rev()
                    .find(|count| self.pending.ends_with(&INIT_PREFIX[..*count]))
                    .unwrap_or(0);
                let emit = self.pending.len().saturating_sub(keep);
                ordinary.extend(self.pending.drain(..emit));
                if self.pending.len() > MAX_DETECT_BUFFER {
                    ordinary.extend(self.pending.drain(..));
                }
                return (ordinary, None);
            };
            if self.pending.len() < index + INIT_PREFIX.len() + 13 {
                if index > 0 {
                    ordinary.extend(self.pending.drain(..index));
                }
                if self.pending.len() > MAX_DETECT_BUFFER {
                    ordinary.extend(self.pending.drain(..));
                }
                return (ordinary, None);
            }
            let signature =
                &self.pending[index + INIT_PREFIX.len()..index + INIT_PREFIX.len() + 13];
            let direction = signature[0];
            let valid = matches!(direction, b'0' | b'1')
                && signature[1..].iter().all(u8::is_ascii_hexdigit);
            if !valid {
                ordinary.extend(self.pending.drain(..index + 1));
                continue;
            }
            ordinary.extend(self.pending.drain(..index));
            let protocol = std::mem::take(&mut self.pending);
            if protocol
                .windows(5)
                .any(|window| window == b"\x18\x18\x18\x18\x18")
                || protocol.windows(12).any(|window| window == b"cannot open ")
            {
                ordinary.extend(protocol);
                return (ordinary, None);
            }
            return (
                ordinary,
                Some((
                    ZmodemInit {
                        upload: direction == b'1',
                    },
                    protocol,
                )),
            );
        }
    }

    pub fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.pending)
    }
}

pub(in crate::filter) struct ZmodemProcess {
    pub child: Child,
    pub stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
}

impl ZmodemProcess {
    pub fn take_stdout(&mut self) -> io::Result<ChildStdout> {
        self.stdout
            .take()
            .ok_or_else(|| io::Error::other("ZMODEM process stdout already consumed"))
    }
}

impl Drop for ZmodemProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start an injectable `sz` or `rz` executable for the requested direction.
pub(in crate::filter) fn spawn_client<P: AsRef<OsStr>>(
    executable: P,
    upload: bool,
    paths: &[std::path::PathBuf],
    download_dir: Option<&Path>,
) -> io::Result<ZmodemProcess> {
    let mut command = Command::new(executable);
    if upload {
        command.args(["-e", "-b", "-B", "32768"]);
        command.args(paths);
    } else {
        command.args(["-E", "-e", "-b", "-B", "32768"]);
        if let Some(directory) = download_dir {
            command.current_dir(directory);
        }
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("ZMODEM process stdin unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("ZMODEM process stdout unavailable"))?;
    Ok(ZmodemProcess {
        child,
        stdin: Some(stdin),
        stdout: Some(stdout),
    })
}

pub(in crate::filter) fn wait_child(child: &mut Child) -> io::Result<ExitStatus> {
    child.wait()
}

fn has_finish_marker(bytes: &[u8]) -> bool {
    bytes.windows(FINISH_PREFIX.len() + 12).any(|window| {
        window.starts_with(FINISH_PREFIX)
            && window[FINISH_PREFIX.len()..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    })
}

#[derive(Default)]
pub(in crate::filter) struct ZmodemFinishDetector {
    pending: Vec<u8>,
    finished: bool,
}

impl ZmodemFinishDetector {
    pub fn push(&mut self, bytes: &[u8]) -> bool {
        if self.finished {
            return true;
        }
        self.pending.extend_from_slice(bytes);
        if has_finish_marker(&self.pending) {
            self.finished = true;
            self.pending.clear();
            return true;
        }
        let max_len = FINISH_PREFIX.len() + 11;
        let keep = (1..=max_len.min(self.pending.len()))
            .rev()
            .find(|length| is_finish_marker_prefix(&self.pending[self.pending.len() - length..]))
            .unwrap_or(0);
        let discard = self.pending.len().saturating_sub(keep);
        self.pending.drain(..discard);
        false
    }
}

fn is_finish_marker_prefix(bytes: &[u8]) -> bool {
    if bytes.len() <= FINISH_PREFIX.len() {
        return FINISH_PREFIX.starts_with(bytes);
    }
    bytes.len() < FINISH_PREFIX.len() + 12
        && bytes.starts_with(FINISH_PREFIX)
        && bytes[FINISH_PREFIX.len()..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

pub(in crate::filter) fn idle_timeout_elapsed(last_activity: Instant, now: Instant) -> bool {
    now.saturating_duration_since(last_activity) >= IDLE_TIMEOUT
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn detects_fragmented_init_and_preserves_non_protocol_data() {
        let mut detector = ZmodemDetector::default();
        let (plain, init) = detector.push(b"shell **\x18B0");
        assert_eq!(plain, b"shell ");
        assert!(init.is_none());
        let (plain, init) = detector.push(b"1000000000000\r\nrest");
        assert!(plain.is_empty());
        let (init, protocol) = init.unwrap();
        assert!(init.upload);
        assert!(protocol.starts_with(b"**\x18B01000000000000"));
        assert!(protocol.ends_with(b"rest"));

        let mut invalid = ZmodemDetector::default();
        let (plain, init) = invalid.push(b"**\x18B0x000000000000");
        assert!(init.is_none());
        assert_eq!(plain, b"**\x18B0x000000000000");
    }

    #[cfg(unix)]
    #[test]
    fn fake_executable_has_two_way_stream_and_can_be_terminated() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("fake-rz.sh");
        std::fs::write(&executable, "#!/bin/sh\ncat\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut process = spawn_client(&executable, false, &[], Some(dir.path())).unwrap();
        process
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"protocol bytes")
            .unwrap();
        process.stdin.take();
        let mut output = Vec::new();
        process
            .take_stdout()
            .unwrap()
            .read_to_end(&mut output)
            .unwrap();
        assert_eq!(output, b"protocol bytes");
        assert!(wait_child(&mut process.child).unwrap().success());

        let mut process = spawn_client(&executable, false, &[], Some(dir.path())).unwrap();
        process.child.kill().unwrap();
        let _ = wait_child(&mut process.child).unwrap();
    }
    #[test]
    fn detects_finish_marker_and_bounds_idle_timeout() {
        assert!(has_finish_marker(b"noise**\x18B08abcdef012345tail"));
        assert!(!has_finish_marker(b"**\x18B08ABCDEF012345"));
        let start = Instant::now();
        assert!(!idle_timeout_elapsed(
            start,
            start + Duration::from_secs(19)
        ));
        assert!(idle_timeout_elapsed(start, start + Duration::from_secs(20)));
    }
    #[test]
    fn finish_detector_handles_every_chunk_boundary() {
        let marker = b"**\x18B08abcdef012345";
        for split in 0..=marker.len() {
            let mut detector = ZmodemFinishDetector::default();
            let first = detector.push(&marker[..split]);
            let second = detector.push(&marker[split..]);
            assert!(first || second, "marker split at byte {split}");
            assert!(detector.push(b"later bytes"));
        }
        let mut detector = ZmodemFinishDetector::default();
        assert!(!detector.push(b"**\x18B08ABCDEF"));
        assert!(!detector.push(b"012345"));
    }
}
