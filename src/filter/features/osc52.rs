use base64::Engine;

const MARKER: &[u8] = b"\x1b]52;";
const MAX_SEQUENCE: usize = 100_000;

#[derive(Default)]
pub(in crate::filter) struct Osc52Parser {
    pending: Vec<u8>,
    payload: Option<Vec<u8>>,
}

impl Osc52Parser {
    /// Feed output bytes and return completed clipboard payloads. Input/output
    /// bytes are never modified by this parser.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.pending.extend_from_slice(bytes);
        let mut completed = Vec::new();
        loop {
            if let Some(payload) = self.payload.as_mut() {
                if let Some(end) = self
                    .pending
                    .iter()
                    .position(|byte| *byte == 0x07 || *byte == 0x1b)
                {
                    payload.extend_from_slice(&self.pending[..end]);
                    self.pending.drain(..=end);
                    if let Some(encoded) = self.payload.take() {
                        if encoded.len() <= MAX_SEQUENCE && encoded.iter().all(is_base64_byte) {
                            if let Ok(decoded) =
                                base64::engine::general_purpose::STANDARD.decode(encoded)
                            {
                                completed.push(decoded);
                            }
                        }
                    }
                    continue;
                }
                payload.extend_from_slice(&self.pending);
                self.pending.clear();
                if payload.len() > MAX_SEQUENCE || !payload.iter().all(is_base64_byte) {
                    self.payload = None;
                }
                return completed;
            }

            let Some(index) = find_subslice(&self.pending, MARKER) else {
                let keep = suffix_prefix_len(&self.pending, MARKER);
                let emit = self.pending.len().saturating_sub(keep);
                self.pending.drain(..emit);
                return completed;
            };
            if self.pending.len() < index + MARKER.len() + 2 {
                self.pending.drain(..index);
                return completed;
            }
            self.pending.drain(..index + MARKER.len());
            if !matches!(self.pending[0], b'c' | b'p') || self.pending[1] != b';' {
                self.pending.drain(..2);
                continue;
            }
            self.pending.drain(..2);
            self.payload = Some(Vec::new());
        }
    }

    pub fn finish(&mut self) {
        self.pending.clear();
        self.payload = None;
    }
}

fn is_base64_byte(byte: &u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn suffix_prefix_len(input: &[u8], marker: &[u8]) -> usize {
    (1..=marker.len().min(input.len()))
        .rev()
        .find(|length| input.ends_with(&marker[..*length]))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn decodes_fragmented_osc52_without_touching_stream() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("clipboard text");
        let mut parser = Osc52Parser::default();
        assert!(parser.push(b"prompt\x1b]52;c;Y2xpcGJvYXJk").is_empty());
        let values = parser.push(b"IHRleHQ=\x07tail");
        assert_eq!(values, vec![b"clipboard text".to_vec()]);
        assert!(
            parser
                .push(format!("\x1b]52;p;{encoded}\x1b\\").as_bytes())
                .len()
                == 1
        );
    }

    #[test]
    fn malformed_and_oversized_sequences_are_ignored() {
        let mut parser = Osc52Parser::default();
        assert!(parser.push(b"\x1b]52;x;YWJj\x07").is_empty());
        assert!(parser.push(b"\x1b]52;c;@@@\x07").is_empty());
        assert!(
            parser
                .push(format!("\x1b]52;c;{}", "A".repeat(MAX_SEQUENCE + 1)).as_bytes())
                .is_empty()
        );
    }
}
