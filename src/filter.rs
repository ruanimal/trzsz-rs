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

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::progress::TextProgressBar;
use crate::transfer::TrzszTransfer;
use crate::version::TrzszVersion;

/// TrzszOptions specify the options to create a TrzszFilter.
#[derive(Debug, Clone, Default)]
pub struct TrzszOptions {
    pub terminal_columns: i32,
    pub detect_drag_file: bool,
    pub detect_trace_log: bool,
    pub enable_zmodem: bool,
    pub enable_osc52: bool,
}

/// Detected trigger from server output.
#[derive(Debug, Clone)]
pub struct TrzszTrigger {
    pub mode: char,
    pub version: Option<TrzszVersion>,
    pub unique_id: String,
    pub win_server: bool,
    pub tunnel_port: i32,
    pub tmux_prefix: String,
    pub tmux_pane_id: String,
}

/// TrzszFilter wraps client/server I/O to support trzsz (trz / tsz).
pub struct TrzszFilter {
    pub client_in: Box<dyn Read + Send>,
    pub client_out: Box<dyn Write + Send>,
    pub server_in: Box<dyn Write + Send>,
    pub server_out: Box<dyn Read + Send>,
    pub options: TrzszOptions,
    pub transfer: Arc<Mutex<Option<TrzszTransfer>>>,
    pub progress: Arc<Mutex<Option<TextProgressBar>>>,
    pub trigger: Option<TrzszTrigger>,
    pub interrupting: AtomicBool,
    pub closed: AtomicBool,
    pub default_upload_path: Option<String>,
    pub default_download_path: Option<String>,
    pub drag_file_upload_command: Option<String>,
}

impl TrzszFilter {
    pub fn new(
        client_in: Box<dyn Read + Send>,
        client_out: Box<dyn Write + Send>,
        server_in: Box<dyn Write + Send>,
        server_out: Box<dyn Read + Send>,
        options: TrzszOptions,
    ) -> Self {
        let filter = TrzszFilter {
            client_in,
            client_out,
            server_in,
            server_out,
            options,
            transfer: Arc::new(Mutex::new(None)),
            progress: Arc::new(Mutex::new(None)),
            trigger: None,
            interrupting: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            default_upload_path: None,
            default_download_path: None,
            drag_file_upload_command: None,
        };
        filter
    }

    pub fn set_terminal_columns(&self, columns: i32) {
        let mut progress = self.progress.lock().unwrap();
        if let Some(ref mut p) = *progress {
            p.columns.store(columns, Ordering::Relaxed);
        }
    }

    pub fn is_transferring_files(&self) -> bool {
        self.transfer.lock().unwrap().is_some()
    }

    pub fn stop_transferring_files(&self, stop_and_delete: bool) {
        if let Some(ref transfer) = *self.transfer.lock().unwrap() {
            transfer.stop_transferring_files(stop_and_delete);
        }
    }

    /// Read trzsz config from ~/.trzsz.conf
    pub fn read_trzsz_config(&mut self) {
        if let Ok(home) = std::env::var("HOME") {
            let config_path = std::path::Path::new(&home).join(".trzsz.conf");
            if let Ok(content) = std::fs::read_to_string(&config_path) {
                for line in content.lines() {
                    let line = if let Some(idx) = line.find('#') {
                        &line[..idx]
                    } else {
                        line
                    };
                    let line = line.trim();
                    if let Some(idx) = line.find('=') {
                        let name = line[..idx].trim().to_lowercase();
                        let value = line[idx + 1..].trim().to_string();
                        if name.is_empty() || value.is_empty() {
                            continue;
                        }
                        match name.as_str() {
                            "defaultuploadpath" if self.default_upload_path.is_none() => {
                                self.default_upload_path = Some(value);
                            }
                            "defaultdownloadpath" if self.default_download_path.is_none() => {
                                self.default_download_path = Some(value);
                            }
                            "dragfileuploadcommand" if self.drag_file_upload_command.is_none() => {
                                self.drag_file_upload_command = Some(value);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    /// Detect trzsz trigger in server output.
    pub fn detect_trzsz(buf: &[u8]) -> (Vec<u8>, Option<TrzszTrigger>) {
        let output = String::from_utf8_lossy(buf);
        let marker = "::TRZSZ:TRANSFER:";
        if let Some(idx) = output.find(marker) {
            let rest = &output[idx + marker.len()..];
            let end_markers = ['\n', '\r'];
            let end = rest
                .find(|c: char| end_markers.contains(&c))
                .unwrap_or(rest.len());
            let trigger_str = &rest[..end];
            let parts: Vec<&str> = trigger_str.split(':').collect();
            if parts.len() >= 3 {
                let mode = parts[0].chars().next().unwrap_or(' ');
                let version = TrzszVersion::parse(parts[1]);
                let unique_id = parts[2].to_string();
                let win_server = unique_id.ends_with('0')
                    || unique_id.ends_with('2')
                    || unique_id.ends_with("10");
                let tunnel_port = if parts.len() > 3 {
                    parts[3].parse::<i32>().unwrap_or(0)
                } else {
                    0
                };
                return (
                    buf.to_vec(),
                    Some(TrzszTrigger {
                        mode,
                        version,
                        unique_id,
                        win_server,
                        tunnel_port,
                        tmux_prefix: String::new(),
                        tmux_pane_id: String::new(),
                    }),
                );
            }
        }
        (buf.to_vec(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_trzsz() {
        let output = b"some output::TRZSZ:TRANSFER:S:1.2.0:1234567890123:0\n";
        let (_buf, trigger) = TrzszFilter::detect_trzsz(output);
        assert!(trigger.is_some());
        let t = trigger.unwrap();
        assert_eq!(t.mode, 'S');
        assert_eq!(
            t.version,
            Some(TrzszVersion {
                major: 1,
                minor: 2,
                patch: 0
            })
        );
        assert_eq!(t.unique_id, "1234567890123");
    }

    #[test]
    fn test_detect_trzsz_no_trigger() {
        let output = b"just normal output\n";
        let (_buf, trigger) = TrzszFilter::detect_trzsz(output);
        assert!(trigger.is_none());
    }

    #[test]
    fn test_detect_trzsz_with_prefix() {
        let output = b"\x1b[s::TRZSZ:TRANSFER:R:1.0.0:0\n";
        let (_buf, trigger) = TrzszFilter::detect_trzsz(output);
        assert!(trigger.is_some());
        let t = trigger.unwrap();
        assert_eq!(t.mode, 'R');
    }
}
