/*
MIT License

Copyright (c) 2022-2026 The Trzsz Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use crate::comm::{
    TrzszError, err_interrupted, err_receive_data_timeout, err_stopped, simple_error,
};

const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub struct TrzszBuffer {
    sender: SyncSender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    next_buf: Option<Vec<u8>>,
    next_idx: usize,
    read_buf: Vec<u8>,
    timeout: Option<Instant>,
    new_timeout: Option<Option<Instant>>,
    stopped: Arc<AtomicBool>,
}

impl TrzszBuffer {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::sync_channel(10000);
        TrzszBuffer {
            sender: tx,
            receiver: rx,
            next_buf: None,
            next_idx: 0,
            read_buf: Vec::new(),
            timeout: None,
            new_timeout: None,
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        self.stopped.clone()
    }

    pub fn sender(&self) -> SyncSender<Vec<u8>> {
        self.sender.clone()
    }

    pub fn add_buffer(&self, buf: &[u8]) {
        let _ = self.sender.send(buf.to_vec());
    }

    pub fn drain_buffer(&mut self) {
        while self.receiver.try_recv().is_ok() {}
    }

    pub fn set_new_timeout(&mut self, timeout: Option<Instant>) {
        self.new_timeout = Some(timeout);
    }

    pub fn next_buffer(&mut self) -> Result<Vec<u8>, TrzszError> {
        if self.is_stopped() {
            return Err(err_stopped());
        }
        if let Some(ref buf) = self.next_buf {
            if self.next_idx < buf.len() {
                // Return remaining slice without clearing next_buf, so the
                // caller can update next_idx and we can continue reading from
                // the same buffer on the next call.
                return Ok(buf[self.next_idx..].to_vec());
            }
        }
        self.next_buf = None;
        self.next_idx = 0;

        loop {
            if self.is_stopped() {
                return Err(err_stopped());
            }
            let wait = match self.timeout {
                Some(timeout) => {
                    let remaining = timeout.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        if let Some(new_timeout) = self.new_timeout.take() {
                            self.timeout = new_timeout;
                            continue;
                        }
                        return Err(err_receive_data_timeout());
                    }
                    remaining.min(STOP_POLL_INTERVAL)
                }
                None => STOP_POLL_INTERVAL,
            };
            match self.receiver.recv_timeout(wait) {
                Ok(buf) => {
                    self.next_buf = Some(buf.clone());
                    self.next_idx = 0;
                    return Ok(buf);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self
                        .timeout
                        .is_some_and(|timeout| Instant::now() >= timeout)
                    {
                        if let Some(new_timeout) = self.new_timeout.take() {
                            self.timeout = new_timeout;
                            continue;
                        }
                        return Err(err_receive_data_timeout());
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(err_stopped()),
            }
        }
    }

    pub fn read_line(
        &mut self,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<Vec<u8>, TrzszError> {
        self.read_line_for(None, may_has_junk, timeout)
    }

    pub fn read_line_for(
        &mut self,
        expect_type: Option<&str>,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<Vec<u8>, TrzszError> {
        self.read_line_for_limited(expect_type, may_has_junk, timeout, usize::MAX)
    }

    pub(crate) fn read_line_for_limited(
        &mut self,
        expect_type: Option<&str>,
        may_has_junk: bool,
        timeout: Option<Instant>,
        max_line_size: usize,
    ) -> Result<Vec<u8>, TrzszError> {
        self.read_buf.clear();
        self.timeout = timeout;
        self.new_timeout = None;
        loop {
            let buf = self.next_buffer()?;
            let newline_idx = buf.iter().position(|&b| b == b'\n');

            if let Some(idx) = newline_idx {
                // `buf` starts at the current read offset, so advance past this line.
                if self.read_buf.len().saturating_add(idx) > max_line_size {
                    return Err(simple_error("Protocol line exceeds maximum size"));
                }
                self.next_idx += idx + 1;
                let line = &buf[..idx];
                self.read_buf.extend_from_slice(line);
                if self.read_buf.contains(&0x03) {
                    return Err(err_interrupted());
                }
                if may_has_junk && self.read_buf.last() == Some(&b'\r') {
                    self.read_buf.pop();
                    continue;
                }
                let mut line = self.read_buf.clone();
                if may_has_junk {
                    if let Some(expect_type) = expect_type {
                        let expected = format!("#{}:", expect_type);
                        if let Some(idx) = find_last_subslice(&line, expected.as_bytes()) {
                            line.drain(..idx);
                        } else if let Some(idx) = line.iter().rposition(|&b| b == b'#') {
                            if idx > 0 {
                                line.drain(..idx);
                            }
                        }
                    }
                    line = strip_tmux_status_line(line);
                }
                return Ok(line);
            }

            // The whole buffer had no newline; consume it before receiving the next chunk.
            if self.read_buf.len().saturating_add(buf.len()) > max_line_size {
                return Err(simple_error("Protocol line exceeds maximum size"));
            }
            self.next_idx += buf.len();
            if buf.contains(&0x03) {
                return Err(err_interrupted());
            }
            self.read_buf.extend_from_slice(&buf);
        }
    }
    pub fn read_binary(
        &mut self,
        size: usize,
        timeout: Option<Instant>,
    ) -> Result<Vec<u8>, TrzszError> {
        self.read_buf.clear();
        self.read_buf.reserve(size);
        self.timeout = timeout;
        self.new_timeout = None;
        while self.read_buf.len() < size {
            let buf = self.next_buffer()?;
            let left = size - self.read_buf.len();
            if buf.len() > left {
                self.next_idx += left;
                self.read_buf.extend_from_slice(&buf[..left]);
            } else {
                self.next_idx += buf.len();
                self.read_buf.extend_from_slice(&buf);
            }
        }
        Ok(self.read_buf.clone())
    }

    pub fn read_line_on_windows(
        &mut self,
        timeout: Option<Instant>,
    ) -> Result<Vec<u8>, TrzszError> {
        self.read_buf.clear();
        self.timeout = timeout;
        self.new_timeout = None;
        let mut skip_vt100 = false;
        let mut last_byte: u8 = 0x1b;
        let mut has_newline = false;
        let mut may_duplicate = false;
        let mut has_cursor_home = false;
        let mut pre_has_cursor_home = false;

        loop {
            let buf = self.next_buffer()?;
            let mut new_line_idx = None;
            for (i, &b) in buf.iter().enumerate() {
                if b == b'!' {
                    new_line_idx = Some(i);
                    break;
                }
            }

            // Advance next_idx so subsequent reads continue past this segment.
            if let Some(idx) = new_line_idx {
                self.next_idx += idx + 1;
                // Skip optional newline after '!'
                if self.next_idx < self.next_buf.as_ref().map(|b| b.len()).unwrap_or(0) {
                    if let Some(ref nb) = self.next_buf {
                        if nb[self.next_idx] == b'\n' {
                            self.next_idx += 1;
                        }
                    }
                }
            } else {
                self.next_idx += buf.len();
            }

            let line_end = new_line_idx.unwrap_or(buf.len());

            let data = &buf[..line_end];

            for &c in data {
                if c == 0x03 {
                    return Err(err_interrupted());
                }
                if c == b'\n' {
                    has_newline = true;
                }
                if skip_vt100 {
                    if crate::comm::is_vt100_end(c) {
                        skip_vt100 = false;
                        if c == b'H' && last_byte >= b'0' && last_byte <= b'9' {
                            may_duplicate = true;
                        }
                    }
                    if last_byte == b'[' && c == b'H' {
                        has_cursor_home = true;
                    }
                    last_byte = c;
                } else if c == 0x1b {
                    skip_vt100 = true;
                    last_byte = c;
                } else if is_trzsz_letter(c) {
                    if may_duplicate {
                        may_duplicate = false;
                        if has_newline && !self.read_buf.is_empty() {
                            let last = *self.read_buf.last().unwrap();
                            if c == last || pre_has_cursor_home {
                                *self.read_buf.last_mut().unwrap() = c;
                                continue;
                            }
                        }
                    }
                    self.read_buf.push(c);
                    pre_has_cursor_home = has_cursor_home;
                    has_cursor_home = false;
                    has_newline = false;
                }
            }

            if new_line_idx.is_some() && !self.read_buf.is_empty() && !skip_vt100 {
                return Ok(self.read_buf.clone());
            }
        }
    }
}

fn find_last_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
}

fn strip_tmux_status_line(mut line: Vec<u8>) -> Vec<u8> {
    loop {
        let Some(begin) = line.windows(3).position(|window| window == b"\x1bP=") else {
            return line;
        };
        let Some(mid_rel) = line[begin + 3..]
            .windows(3)
            .position(|window| window == b"\x1bP=")
        else {
            line.truncate(begin);
            return line;
        };
        let mid = begin + 3 + mid_rel + 3;
        let Some(end_rel) = line[mid..]
            .windows(2)
            .position(|window| window == b"\x1b\\")
        else {
            line.truncate(begin);
            return line;
        };
        line.drain(begin..mid + end_rel + 2);
    }
}

fn is_trzsz_letter(b: u8) -> bool {
    (b >= b'a' && b <= b'z')
        || (b >= b'A' && b <= b'Z')
        || (b >= b'0' && b <= b'9')
        || b == b'#'
        || b == b':'
        || b == b'+'
        || b == b'/'
        || b == b'='
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_trzsz_letter() {
        assert!(is_trzsz_letter(b'a'));
        assert!(is_trzsz_letter(b'z'));
        assert!(is_trzsz_letter(b'A'));
        assert!(is_trzsz_letter(b'Z'));
        assert!(is_trzsz_letter(b'0'));
        assert!(is_trzsz_letter(b'9'));
        assert!(is_trzsz_letter(b'#'));
        assert!(is_trzsz_letter(b':'));
        assert!(!is_trzsz_letter(b' '));
        assert!(!is_trzsz_letter(b'\n'));
        assert!(!is_trzsz_letter(0x1b));
    }

    #[test]
    fn test_read_line_then_binary_in_one_chunk() {
        // Regression test: a single received chunk contains both a line
        // (e.g. "#DATA:5\n") and the binary payload that follows. The buffer
        // must not lose the bytes after the newline.
        let mut buf = TrzszBuffer::new();
        let mut data = Vec::new();
        data.extend_from_slice(b"#DATA:5\n");
        data.extend_from_slice(b"hello");
        data.extend_from_slice(b"#SUCC:5\n");
        buf.add_buffer(&data);

        let line = buf.read_line(false, None).unwrap();
        assert_eq!(line, b"#DATA:5");

        let bin = buf.read_binary(5, None).unwrap();
        assert_eq!(bin, b"hello");

        let line2 = buf.read_line(false, None).unwrap();
        assert_eq!(line2, b"#SUCC:5");
    }

    #[test]
    fn test_read_multiple_lines_in_one_chunk() {
        // Regression test: a single received chunk contains multiple lines.
        // The buffer must return them one at a time.
        let mut buf = TrzszBuffer::new();
        buf.add_buffer(b"#A:1\n#B:2\n#C:3\n");

        assert_eq!(buf.read_line(false, None).unwrap(), b"#A:1");
        assert_eq!(buf.read_line(false, None).unwrap(), b"#B:2");
        assert_eq!(buf.read_line(false, None).unwrap(), b"#C:3");
    }

    #[test]
    fn test_read_binary_across_chunks() {
        // Binary read must reassemble bytes from multiple incoming chunks.
        let mut buf = TrzszBuffer::new();
        buf.add_buffer(b"#DATA:10\nabc");
        buf.add_buffer(b"defgh");
        buf.add_buffer(b"ij#NEXT:1\n");

        assert_eq!(buf.read_line(false, None).unwrap(), b"#DATA:10");
        assert_eq!(buf.read_binary(10, None).unwrap(), b"abcdefghij");
        assert_eq!(buf.read_line(false, None).unwrap(), b"#NEXT:1");
    }
    #[test]
    fn read_line_discards_crlf_protocol_line_in_same_chunk() {
        let mut buf = TrzszBuffer::new();
        buf.add_buffer(b"#ACT:discarded\r\n#ACT:ready\n");

        assert_eq!(
            buf.read_line_for(Some("ACT"), true, None).unwrap(),
            b"#ACT:ready"
        );
    }

    #[test]
    fn read_line_discards_crlf_protocol_line_across_chunks() {
        let mut buf = TrzszBuffer::new();
        buf.add_buffer(b"#ACT:discarded\r");
        buf.add_buffer(b"\n#ACT:ready\n");

        assert_eq!(
            buf.read_line_for(Some("ACT"), true, None).unwrap(),
            b"#ACT:ready"
        );
    }
}
