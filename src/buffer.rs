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

use std::io::{Read};
use std::sync::mpsc::{self, SyncSender, Receiver};
use std::time::{Duration, Instant};

use crate::comm::{err_interrupted, err_receive_data_timeout, err_stopped, TrzszError};

pub struct TrzszBuffer {
    sender: SyncSender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    drain_receiver: Receiver<()>,
    next_buf: Option<Vec<u8>>,
    next_idx: usize,
    read_buf: Vec<u8>,
    timeout: Option<Instant>,
    new_timeout: Option<Option<Instant>>,
}

impl TrzszBuffer {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::sync_channel(10000);
        let (drain_tx, drain_rx) = mpsc::sync_channel(10000);
        TrzszBuffer {
            sender: tx,
            receiver: rx,
            drain_receiver: drain_rx,
            next_buf: None,
            next_idx: 0,
            read_buf: Vec::new(),
            timeout: None,
            new_timeout: None,
        }
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

    fn pop_buffer(&mut self) -> Option<&[u8]> {
        let has_data = self.next_buf.as_ref().map_or(false, |buf| self.next_idx < buf.len());
        if has_data {
            return self.next_buf.as_deref().map(|buf| &buf[self.next_idx..]);
        }
        self.next_buf = None;
        self.next_idx = 0;
        None
    }

    pub fn next_buffer(&mut self) -> Result<Vec<u8>, TrzszError> {
        if let Some(ref buf) = self.next_buf {
            if self.next_idx < buf.len() {
                let result = buf[self.next_idx..].to_vec();
                self.next_buf = None;
                self.next_idx = 0;
                return Ok(result);
            }
        }
        self.next_buf = None;
        self.next_idx = 0;

        loop {
            if let Some(timeout) = self.timeout {
                match self.receiver.recv_timeout(timeout.duration_since(Instant::now())) {
                    Ok(buf) => {
                        self.next_buf = Some(buf.clone());
                        self.next_idx = 0;
                        return Ok(buf);
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Some(new_timeout) = self.new_timeout.take() {
                            self.timeout = new_timeout;
                            continue;
                        }
                        return Err(err_receive_data_timeout());
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(err_stopped());
                    }
                }
            } else {
                return match self.receiver.recv() {
                    Ok(buf) => {
                        self.next_buf = Some(buf.clone());
                        self.next_idx = 0;
                        Ok(buf)
                    }
                    Err(_) => Err(err_stopped()),
                };
            }
        }
    }

    pub fn read_line(&mut self, may_has_junk: bool, timeout: Option<Instant>) -> Result<Vec<u8>, TrzszError> {
        self.read_buf.clear();
        self.timeout = timeout;
        self.new_timeout = None;
        loop {
            let buf = self.next_buffer()?;
            let newline_idx = buf.iter().position(|&b| b == b'\n');

            if let Some(idx) = newline_idx {
                self.next_idx = idx + 1;
                let line = &buf[..idx];
                if may_has_junk && !self.read_buf.is_empty() && self.read_buf.last() == Some(&b'\r') {
                    self.read_buf.truncate(self.read_buf.len() - 1);
                    self.read_buf.extend_from_slice(&buf[..idx]);
                    continue;
                }
                self.read_buf.extend_from_slice(line);
                if self.read_buf.contains(&0x03) {
                    return Err(err_interrupted());
                }
                return Ok(self.read_buf.clone());
            }

            if buf.contains(&0x03) {
                return Err(err_interrupted());
            }
            self.read_buf.extend_from_slice(&buf);
        }
    }

    pub fn read_binary(&mut self, size: usize, timeout: Option<Instant>) -> Result<Vec<u8>, TrzszError> {
        self.read_buf.clear();
        self.read_buf.reserve(size);
        self.timeout = timeout;
        self.new_timeout = None;
        while self.read_buf.len() < size {
            let buf = self.next_buffer()?;
            let left = size - self.read_buf.len();
            if buf.len() > left {
                self.next_idx = left;
                self.read_buf.extend_from_slice(&buf[..left]);
            } else {
                self.next_buf = None;
                self.next_idx = 0;
                self.read_buf.extend_from_slice(&buf);
            }
        }
        Ok(self.read_buf.clone())
    }

    pub fn read_line_on_windows(&mut self, timeout: Option<Instant>) -> Result<Vec<u8>, TrzszError> {
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

            let line_end = new_line_idx.unwrap_or(buf.len());
            let remaining = if let Some(idx) = new_line_idx {
                let mut rest = &buf[idx + 1..];
                if !rest.is_empty() && rest[0] == b'\n' {
                    rest = &rest[1..];
                }
                Some(rest)
            } else {
                None
            };

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
}
