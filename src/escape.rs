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

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use std::io::{Read, Write};

use crate::comm::{TrzszError, simple_trzsz_error};

pub const ESCAPE_LEADER_BYTE: u8 = 0xee;

/// Encode bytes to base64(zlib(data)).
pub fn encode_bytes(buf: &[u8]) -> String {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    let _ = encoder.write_all(buf);
    let compressed = encoder.finish().unwrap_or_default();
    BASE64.encode(&compressed)
}

/// Encode a string to base64(zlib(data)).
pub fn encode_string(s: &str) -> String {
    encode_bytes(s.as_bytes())
}

/// Decode base64(zlib(data)) to bytes.
pub fn decode_string(s: &str) -> Result<Vec<u8>, TrzszError> {
    let decoded = BASE64
        .decode(s)
        .map_err(|e| simple_trzsz_error("Base64 decode error", e))?;
    let mut decoder = ZlibDecoder::new(&decoded[..]);
    let mut result = Vec::new();
    decoder
        .read_to_end(&mut result)
        .map_err(|e| simple_trzsz_error("Zlib decode error", e))?;
    Ok(result)
}

/// Escape table for binary mode.
#[derive(Debug, Clone, Default)]
pub struct EscapeTable {
    pub total_count: usize,
    pub escape_codes: Vec<Option<u8>>, // 256 entries: original byte → replacement byte
    pub unescape_codes: Vec<Option<u8>>, // 256 entries: escaped byte → original byte
}

impl EscapeTable {
    pub fn new() -> Self {
        EscapeTable {
            total_count: 0,
            escape_codes: vec![None; 256],
            unescape_codes: vec![None; 256],
        }
    }
}

/// Build escape chars list (equivalent to getEscapeChars in Go).
pub fn get_escape_chars(escape_all: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut escape_chars: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![0xee], vec![0xee, 0xee]),
        (vec![0x7e], vec![0xee, 0x31]),
    ];
    if escape_all {
        let chars = [
            0x02, 0x0d, 0x10, 0x11, 0x13, 0x18, 0x1b, 0x1d, 0x8d, 0x90, 0x91, 0x93, 0x9d,
        ];
        let mut e = b'A';
        for &c in &chars {
            escape_chars.push((vec![c], vec![ESCAPE_LEADER_BYTE, e]));
            e += 1;
        }
    }
    escape_chars
}

/// Build an escape table from JSON-parsed escape chars array.
pub fn escape_chars_to_table(
    escape_chars: &[serde_json::Value],
) -> Result<EscapeTable, TrzszError> {
    let mut table = EscapeTable::new();
    table.total_count = escape_chars.len();

    for v in escape_chars {
        let arr = v
            .as_array()
            .ok_or_else(|| simple_trzsz_error("Escape chars invalid", format!("{:?}", v)))?;
        if arr.len() != 2 {
            return Err(simple_trzsz_error(
                "Escape chars invalid",
                format!("{:?}", v),
            ));
        }
        let from_str = arr[0]
            .as_str()
            .ok_or_else(|| simple_trzsz_error("Escape chars invalid", format!("{:?}", v)))?;
        let to_str = arr[1]
            .as_str()
            .ok_or_else(|| simple_trzsz_error("Escape chars invalid", format!("{:?}", v)))?;

        let from_bytes = from_str.as_bytes();
        let to_bytes = to_str.as_bytes();

        if from_bytes.len() != 1 {
            return Err(simple_trzsz_error(
                "Escape chars invalid",
                format!("{:?}", v),
            ));
        }
        if to_bytes.len() != 2 || to_bytes[0] != ESCAPE_LEADER_BYTE {
            return Err(simple_trzsz_error(
                "Escape chars invalid",
                format!("{:?}", v),
            ));
        }

        table.escape_codes[from_bytes[0] as usize] = Some(to_bytes[1]);
        table.unescape_codes[to_bytes[1] as usize] = Some(from_bytes[0]);
    }
    Ok(table)
}

/// Escape data using the escape table.
pub fn escape_data(data: &[u8], table: &EscapeTable) -> Vec<u8> {
    if table.total_count == 0 {
        return data.to_vec();
    }
    let mut buf = Vec::with_capacity(data.len() * 2);
    for &b in data {
        if let Some(ecode) = table.escape_codes[b as usize] {
            buf.push(ESCAPE_LEADER_BYTE);
            buf.push(ecode);
        } else {
            buf.push(b);
        }
    }
    buf
}

/// Unescape data using the escape table.
pub fn unescape_data(
    data: &[u8],
    table: &EscapeTable,
    dst: Option<&mut Vec<u8>>,
) -> Result<(Vec<u8>, Vec<u8>), TrzszError> {
    if table.total_count == 0 {
        return Ok((data.to_vec(), vec![]));
    }
    let size = data.len();
    let mut buf = dst
        .map(|d| {
            d.clear();
            d.clone()
        })
        .unwrap_or_else(|| Vec::with_capacity(size));
    let mut idx = 0;
    while idx < size {
        if data[idx] == ESCAPE_LEADER_BYTE {
            if idx == size - 1 {
                return Ok((buf, data[idx..].to_vec()));
            }
            idx += 1;
            if let Some(ecode) = table.unescape_codes[data[idx] as usize] {
                buf.push(ecode);
            } else {
                return Err(simple_trzsz_error(
                    "Unknown escape code",
                    format!("{}", data[idx]),
                ));
            }
        } else {
            buf.push(data[idx]);
        }
        idx += 1;
    }
    Ok((buf, vec![]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = b"Hello, world! This is a test string with some special chars: \x00\x01\xff";
        let encoded = encode_bytes(original);
        let decoded = decode_string(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_string_roundtrip() {
        let original = "Hello, world!";
        let encoded = encode_string(original);
        let decoded = decode_string(&encoded).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), original);
    }

    #[test]
    fn test_escape_unescape_roundtrip() {
        let table = EscapeTable {
            total_count: 2,
            escape_codes: {
                let mut v = vec![None; 256];
                v[0x7e] = Some(0x31);
                v[0xee] = Some(0xee);
                v
            },
            unescape_codes: {
                let mut v = vec![None; 256];
                v[0x31] = Some(0x7e);
                v[0xee] = Some(0xee);
                v
            },
        };
        let data = b"Hello~World\xeeTest";
        let escaped = escape_data(data, &table);
        let (unescaped, remaining) = unescape_data(&escaped, &table, None).unwrap();
        assert_eq!(unescaped, data);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_get_escape_chars() {
        let chars = get_escape_chars(false);
        assert_eq!(chars.len(), 2);

        let chars_all = get_escape_chars(true);
        assert!(chars_all.len() > 2);
    }
}
