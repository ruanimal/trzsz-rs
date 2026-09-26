use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_LINE: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::filter) enum TmuxClientInput {
    CurrentPane(Vec<u8>),
    Forward(Vec<u8>),
}

#[derive(Default)]
pub(in crate::filter) struct TmuxInputDecoder {
    pending: Vec<u8>,
}

impl TmuxInputDecoder {
    pub fn feed(&mut self, input: &[u8], pane_id: &str) -> Vec<TmuxClientInput> {
        self.pending.extend_from_slice(input);
        if self.pending.len() > MAX_LINE {
            return vec![TmuxClientInput::Forward(std::mem::take(&mut self.pending))];
        }
        let mut decoded = Vec::new();
        while let Some(end) = self.pending.iter().position(|byte| *byte == b'\r') {
            let mut line: Vec<u8> = self.pending.drain(..=end).collect();
            line.pop();
            for command in line.split(|byte| *byte == b';') {
                let command = trim_ascii(command);
                if !command.is_empty() {
                    decoded.push(decode_client_command(command, pane_id));
                }
            }
        }
        decoded
    }

    pub fn finish(&mut self) -> Option<TmuxClientInput> {
        if self.pending.is_empty() {
            None
        } else {
            Some(TmuxClientInput::Forward(std::mem::take(&mut self.pending)))
        }
    }
}

fn decode_client_command(command: &[u8], current_pane: &str) -> TmuxClientInput {
    let Some(rest) = command.strip_prefix(b"send ") else {
        return TmuxClientInput::Forward(command.to_vec());
    };
    let Some(option_end) = rest.iter().position(u8::is_ascii_whitespace) else {
        return TmuxClientInput::Forward(command.to_vec());
    };
    let option = &rest[..option_end];
    if !matches!(option, b"-lt" | b"-t") {
        return TmuxClientInput::Forward(command.to_vec());
    }
    let after_option = trim_ascii_start(&rest[option_end..]);
    let after_option = after_option.strip_prefix(b" ").unwrap_or(after_option);
    let pane_end = after_option
        .iter()
        .position(u8::is_ascii_whitespace)
        .unwrap_or(after_option.len());
    let pane = &after_option[..pane_end];
    if pane != current_pane.as_bytes() {
        return TmuxClientInput::Forward(command.to_vec());
    }
    let data = after_option
        .get(pane_end..)
        .map(trim_ascii_start)
        .unwrap_or_default();
    if option == b"-lt" {
        return TmuxClientInput::CurrentPane(data.to_vec());
    }
    let mut bytes = Vec::new();
    for token in data
        .split(u8::is_ascii_whitespace)
        .filter(|token| !token.is_empty())
    {
        if token == b"C-Space" {
            bytes.push(0);
            continue;
        }
        let Some(hex) = token.strip_prefix(b"0x") else {
            return TmuxClientInput::Forward(command.to_vec());
        };
        let Ok(hex) = std::str::from_utf8(hex) else {
            return TmuxClientInput::Forward(command.to_vec());
        };
        let Ok(byte) = u8::from_str_radix(hex, 16) else {
            return TmuxClientInput::Forward(command.to_vec());
        };
        bytes.push(byte);
    }
    TmuxClientInput::CurrentPane(bytes)
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    bytes = trim_ascii_start(bytes);
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn trim_ascii_start(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    bytes
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::filter) enum TmuxRecord {
    Output {
        pane_id: String,
        prefix: String,
        bytes: Vec<u8>,
        raw_line: Vec<u8>,
    },
    Visible(Vec<u8>),
}

#[derive(Clone, Default)]
pub(in crate::filter) struct TmuxAck(Arc<(Mutex<AckState>, Condvar)>);

#[derive(Default)]
struct AckState {
    issued: u64,
    completed: u64,
    pending: VecDeque<bool>,
}

impl TmuxAck {
    pub(in crate::filter) fn issue(&self, wait_for_ack: bool) -> Option<u64> {
        let (lock, _) = &*self.0;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if wait_for_ack {
            state.issued = state.issued.saturating_add(1);
            state.pending.push_back(true);
            Some(state.issued)
        } else {
            state.pending.push_back(false);
            None
        }
    }

    pub fn acknowledge(&self, flag: u64) -> bool {
        if flag & 1 == 0 {
            return false;
        }
        let (lock, ready) = &*self.0;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match state.pending.pop_front() {
            Some(true) => {
                state.completed += 1;
                ready.notify_all();
                true
            }
            Some(false) | None => false,
        }
    }

    fn wait(&self, sequence: u64) -> io::Result<()> {
        let (lock, ready) = &*self.0;
        let state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (state, timeout) = ready
            .wait_timeout_while(state, Duration::from_secs(10), |state| {
                state.completed < sequence
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.completed >= sequence {
            Ok(())
        } else if timeout.timed_out() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "tmux control-mode send ack timed out",
            ))
        } else {
            Err(io::Error::other("tmux control-mode send was interrupted"))
        }
    }
}

#[derive(Default)]
pub(in crate::filter) struct TmuxControlDecoder {
    pending: Vec<u8>,
    begin: Option<Vec<u8>>,
    active: bool,
    pub ack: TmuxAck,
}

impl TmuxControlDecoder {
    pub fn should_parse(&self, input: &[u8]) -> bool {
        if self.active || !self.pending.is_empty() {
            return true;
        }
        [
            b"%output ".as_slice(),
            b"%extended-output ".as_slice(),
            b"%begin ".as_slice(),
            b"%end ".as_slice(),
            b"%error ".as_slice(),
            b"%message ".as_slice(),
        ]
        .iter()
        .any(|prefix| prefix.starts_with(input) || input.starts_with(prefix))
    }
    pub fn feed(&mut self, input: &[u8]) -> Vec<TmuxRecord> {
        self.pending.extend_from_slice(input);
        let mut records = Vec::new();
        while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.pending.drain(..=end).collect();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if let Some(record) = self.handle_line(&line) {
                records.push(record);
            }
        }
        if self.pending.len() > MAX_LINE {
            let pending = std::mem::take(&mut self.pending);
            records.push(TmuxRecord::Visible(pending));
        }
        records
    }

    pub fn finish(&mut self) -> Vec<TmuxRecord> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let line = std::mem::take(&mut self.pending);
        self.handle_line(&line).into_iter().collect()
    }

    fn handle_line(&mut self, line: &[u8]) -> Option<TmuxRecord> {
        if line.starts_with(b"%output ") || line.starts_with(b"%extended-output ") {
            self.active = true;
            if let Some((pane_id, prefix, bytes)) = decode_output_line(line) {
                let mut raw_line = line.to_vec();
                raw_line.push(b'\n');
                return Some(TmuxRecord::Output {
                    pane_id,
                    prefix,
                    bytes,
                    raw_line,
                });
            }
            let mut visible = line.to_vec();
            visible.push(b'\n');
            return Some(TmuxRecord::Visible(visible));
        }
        if line.starts_with(b"%begin ") {
            self.active = true;
            self.begin = Some(line.to_vec());
            return None;
        }
        if line.starts_with(b"%end ") || line.starts_with(b"%error ") {
            self.active = true;
            let flag = line
                .split(|byte| byte.is_ascii_whitespace())
                .nth(3)
                .and_then(|token| std::str::from_utf8(token).ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1);
            let mut visible = Vec::new();
            if let Some(begin) = self.begin.take() {
                if self.ack.acknowledge(flag) {
                    return None;
                }
                visible.extend_from_slice(&begin);
                visible.push(b'\n');
            }
            visible.extend_from_slice(line);
            visible.push(b'\n');
            return Some(TmuxRecord::Visible(visible));
        }
        if self.begin.is_some() {
            if let Some(begin) = self.begin.as_mut() {
                begin.push(b'\n');
                begin.extend_from_slice(line);
            }
            return None;
        }
        let mut visible = line.to_vec();
        visible.push(b'\n');
        Some(TmuxRecord::Visible(visible))
    }
}

fn decode_output_line(line: &[u8]) -> Option<(String, String, Vec<u8>)> {
    let (extended, rest) = if let Some(rest) = line.strip_prefix(b"%output ") {
        (false, rest)
    } else if let Some(rest) = line.strip_prefix(b"%extended-output ") {
        (true, rest)
    } else {
        return None;
    };
    let pane_end = rest.iter().position(|byte| *byte == b' ')?;
    let pane = &rest[..pane_end];
    if pane.is_empty() || pane.first() != Some(&b'%') {
        return None;
    }
    let encoded = &rest[pane_end + 1..];
    let (payload, output_prefix) = if extended {
        let colon = encoded.iter().position(|byte| *byte == b':')?;
        let payload_start = colon.checked_add(1)?;
        let payload_start = if encoded.get(payload_start) == Some(&b' ') {
            payload_start + 1
        } else {
            payload_start
        };
        let pane_text = std::str::from_utf8(pane).ok()?;
        (
            &encoded[payload_start..],
            format!("%extended-output {pane_text} 0 : "),
        )
    } else {
        let pane_text = std::str::from_utf8(pane).ok()?;
        (encoded, format!("%output {pane_text} "))
    };
    Some((
        std::str::from_utf8(pane).ok()?.to_string(),
        output_prefix,
        decode_octal(payload)?,
    ))
}

fn decode_octal(encoded: &[u8]) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(encoded.len());
    let mut i = 0;
    while i < encoded.len() {
        if encoded[i] == b'\\' {
            let digits = encoded.get(i + 1..i + 4)?;
            if !digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
                return None;
            }
            let value = ((digits[0] - b'0') as u16) * 64
                + ((digits[1] - b'0') as u16) * 8
                + (digits[2] - b'0') as u16;
            output.push(value as u8);
            i += 4;
        } else {
            output.push(encoded[i]);
            i += 1;
        }
    }
    Some(output)
}

pub(in crate::filter) fn client_input_ack() -> Vec<u8> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("%begin {timestamp} 1 1\r\n%end {timestamp} 1 1\r\n").into_bytes()
}

pub(in crate::filter) fn encode_tmux_output(prefix: &str, output: &[u8]) -> Vec<u8> {
    let mut result = prefix.as_bytes().to_vec();
    for byte in output {
        if *byte < b' ' || *byte == b'\\' || *byte > b'~' {
            result.extend_from_slice(format!("\\{byte:03o}").as_bytes());
        } else {
            result.push(*byte);
        }
    }
    result.extend_from_slice(b"\r\n");
    result
}

pub(in crate::filter) struct TmuxControlWriter<W> {
    inner: W,
    pane_id: String,
    ack: TmuxAck,
}

impl<W: Write> TmuxControlWriter<W> {
    pub fn new(inner: W, pane_id: String, ack: TmuxAck) -> Self {
        TmuxControlWriter {
            inner,
            pane_id,
            ack,
        }
    }

    fn send_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        let mut command = format!("send -t {}", self.pane_id);
        for byte in chunk {
            command.push_str(&format!(" 0x{byte:x}"));
        }
        command.push('\r');
        let sequence = self
            .ack
            .issue(true)
            .ok_or_else(|| io::Error::other("missing tmux ack sequence"))?;
        self.inner.write_all(command.as_bytes())?;
        self.inner.flush()?;
        self.ack.wait(sequence)
    }
}

impl<W: Write> Write for TmuxControlWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        for chunk in bytes.chunks(120) {
            self.send_chunk(chunk)?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reencodes_extended_output_with_a_tmux_valid_control_prefix() {
        let (pane_id, prefix, output) =
            decode_output_line(b"%extended-output %1 42 1: hello\\001").unwrap();
        assert_eq!(pane_id, "%1");
        assert_eq!(prefix, "%extended-output %1 0 : ");
        assert_eq!(output, b"hello\x01");
    }

    #[test]
    fn decodes_partial_tmux_output_and_routes_control_lines() {
        let mut decoder = TmuxControlDecoder::default();
        assert!(decoder.feed(b"%output %2 hello\\0").is_empty());
        let records = decoder.feed(b"01world\r\n%message done\n");
        assert_eq!(
            records[0],
            TmuxRecord::Output {
                pane_id: "%2".to_string(),
                prefix: "%output %2 ".to_string(),
                bytes: b"hello\x01world".to_vec(),
                raw_line: b"%output %2 hello\\001world\n".to_vec(),
            }
        );
        assert_eq!(records[1], TmuxRecord::Visible(b"%message done\n".to_vec()));
    }

    #[test]
    fn current_command_ack_is_consumed_and_other_lines_are_visible() {
        let mut decoder = TmuxControlDecoder::default();
        let issued = decoder.ack.issue(true).unwrap();
        assert_eq!(issued, 1);
        assert!(decoder.feed(b"%begin 1 1 1\n%end 1 1 1 1\n").is_empty());
        decoder.ack.wait(issued).unwrap();
        assert_eq!(
            decoder.feed(b"%message visible\n"),
            vec![TmuxRecord::Visible(b"%message visible\n".to_vec())]
        );
    }

    #[test]
    fn tmux_client_send_commands_decode_literal_hex_and_other_panes() {
        let mut decoder = TmuxInputDecoder::default();
        assert!(decoder.feed(b"send -lt %1 hel", "%1").is_empty());
        assert_eq!(
            decoder.feed(b"lo\rsend -t %1 0x41 0x3\rsend -lt %2 other\r", "%1"),
            vec![
                TmuxClientInput::CurrentPane(b"hello".to_vec()),
                TmuxClientInput::CurrentPane(b"A\x03".to_vec()),
                TmuxClientInput::Forward(b"send -lt %2 other".to_vec()),
            ]
        );
        let ack = client_input_ack();
        assert!(ack.starts_with(b"%begin "));
        assert!(ack.windows(5).any(|window| window == b"%end "));
    }

    #[test]
    fn tmux_writer_frames_protocol_bytes_as_send_commands() {
        #[derive(Clone)]
        struct Writer(Arc<Mutex<Vec<u8>>>);
        impl Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let output = Arc::new(Mutex::new(Vec::new()));
        let ack = TmuxAck::default();
        let reader_ack = ack.clone();
        let waiter = std::thread::spawn(move || {
            while reader_ack.0.0.lock().unwrap().issued == 0 {
                std::thread::yield_now();
            }
            reader_ack.acknowledge(1);
        });
        let mut writer = TmuxControlWriter::new(Writer(output.clone()), "%5".into(), ack);
        writer.write_all(b"A\n").unwrap();
        waiter.join().unwrap();
        assert_eq!(&*output.lock().unwrap(), b"send -t %5 0x41 0xa\r");
    }

    #[test]
    fn rejects_invalid_octal_without_panicking() {
        assert!(decode_octal(b"bad\\xyz").is_none());
        assert_eq!(
            encode_tmux_output("%output %1 ", b"x\n"),
            b"%output %1 x\\012\r\n"
        );
    }
}
