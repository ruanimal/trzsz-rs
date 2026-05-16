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

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use sha2::Digest;
use md5::Md5;

use crate::buffer::TrzszBuffer;
use crate::comm::{
    CompressType, FileWriter, FileReader, SimpleFileReader, SimpleFileWriter,
    SourceFile, TrzszError, err_stopped,
    get_new_name, write_all,
};
use crate::escape::{self, EscapeTable};
use crate::version::{TRZSZ_VERSION, TrzszVersion};
use crate::progress::ProgressCallback;

pub const K_PROTOCOL_VERSION2: i32 = 2;
pub const K_PROTOCOL_VERSION3: i32 = 3;
pub const K_PROTOCOL_VERSION4: i32 = 4;
// Use protocol version 1 for now: V2+ requires streaming encode/decode pipeline
// which is not yet implemented. V1 uses simple stop-and-wait with per-chunk ack.
pub const K_PROTOCOL_VERSION: i32 = 1;
pub const K_LAST_CHUNK_TIME_COUNT: usize = 10;

// ─── Transfer Action (JSON over protocol) ──────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransferAction {
    #[serde(default = "default_lang")]
    pub lang: String,
    #[serde(default)]
    pub version: String,
    #[serde(default = "default_true")]
    pub confirm: bool,
    #[serde(default = "default_newline")]
    pub newline: String,
    #[serde(default)]
    pub protocol: i32,
    #[serde(default = "default_true")]
    #[serde(rename = "binary")]
    pub support_binary: bool,
    #[serde(default)]
    #[serde(rename = "support_dir")]
    pub support_directory: bool,
    #[serde(default)]
    pub tunnel: bool,
    #[serde(default)]
    pub fork: bool,
    #[serde(default)]
    #[serde(rename = "tmuxcc")]
    pub tmux_integration: bool,
}

impl Default for TransferAction {
    fn default() -> Self {
        TransferAction {
            lang: default_lang(),
            version: String::new(),
            confirm: default_true(),
            newline: default_newline(),
            protocol: 0,
            support_binary: default_true(),
            support_directory: false,
            tunnel: false,
            fork: false,
            tmux_integration: false,
        }
    }
}

fn default_lang() -> String { "go".to_string() }
fn default_true() -> bool { true }
fn default_newline() -> String { "\n".to_string() }

// ─── Transfer Config ───────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransferConfig {
    #[serde(default)]
    pub quiet: bool,
    #[serde(default)]
    pub binary: bool,
    #[serde(default)]
    pub directory: bool,
    #[serde(default)]
    pub overwrite: bool,
    #[serde(default = "default_timeout")]
    pub timeout: i32,
    #[serde(default = "default_newline")]
    pub newline: String,
    #[serde(default)]
    pub protocol: i32,
    #[serde(default = "default_bufsize")]
    pub bufsize: i64,
    #[serde(default)]
    pub escape_chars: Option<serde_json::Value>,
    #[serde(default)]
    pub tmux_pane_width: i32,
    #[serde(default)]
    pub tmux_output_junk: bool,
    #[serde(default)]
    pub compress: i32,
    #[serde(default)]
    pub fork: bool,
}

impl Default for TransferConfig {
    fn default() -> Self {
        TransferConfig {
            quiet: false,
            binary: false,
            directory: false,
            overwrite: false,
            timeout: default_timeout(),
            newline: default_newline(),
            protocol: 0,
            bufsize: default_bufsize(),
            escape_chars: None,
            tmux_pane_width: 0,
            tmux_output_junk: false,
            compress: 0,
            fork: false,
        }
    }
}

fn default_timeout() -> i32 { 20 }
fn default_bufsize() -> i64 { 10 * 1024 * 1024 }

// ─── TrzszTransfer ─────────────────────────────────────────────────────────

pub struct TrzszTransfer {
    pub buffer: TrzszBuffer,
    pub writer: Box<dyn Write + Send>,
    pub stopped: AtomicBool,
    pub stop_and_delete: AtomicBool,
    pub term_reseted: AtomicBool,
    pub clean_timeout: Duration,
    pub last_input_time: AtomicI64,
    pub last_chunk_time_arr: [Duration; K_LAST_CHUNK_TIME_COUNT],
    pub last_chunk_time_idx: AtomicU32,
    pub stdin_state: Option<()>,
    pub file_name_map: HashMap<i32, String>,
    pub windows_protocol: bool,
    pub flush_in_time: bool,
    pub transfer_config: TransferConfig,
    pub created_files: Vec<String>,
    pub tunnel_connected: bool,
    pub bg_chan: mpsc::SyncSender<()>,
}

impl TrzszTransfer {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        let (bg_tx, _) = mpsc::sync_channel(1);
        TrzszTransfer {
            buffer: TrzszBuffer::new(),
            writer,
            stopped: AtomicBool::new(false),
            stop_and_delete: AtomicBool::new(false),
            term_reseted: AtomicBool::new(false),
            clean_timeout: Duration::from_millis(100),
            last_input_time: AtomicI64::new(0),
            last_chunk_time_arr: [Duration::ZERO; K_LAST_CHUNK_TIME_COUNT],
            last_chunk_time_idx: AtomicU32::new(0),
            stdin_state: None,
            file_name_map: HashMap::new(),
            windows_protocol: false,
            flush_in_time: false,
            transfer_config: TransferConfig {
                timeout: 20,
                newline: "\n".to_string(),
                bufsize: 10 * 1024 * 1024,
                ..Default::default()
            },
            created_files: Vec::new(),
            tunnel_connected: false,
            bg_chan: bg_tx,
        }
    }

    pub fn background(&self) -> mpsc::Receiver<()> {
        let _ = self.bg_chan.clone(); // keep sender alive
        let (_, rx) = mpsc::sync_channel(1);
        rx
    }

    pub fn add_received_data(&self, buf: &[u8], _tunnel: bool) {
        self.buffer.add_buffer(buf);
        self.last_input_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            Ordering::Relaxed,
        );
    }

    pub fn stop_transferring_files(&self, stop_and_delete: bool) {
        if !self.stopped.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            return;
        }
        self.stop_and_delete.store(stop_and_delete, Ordering::Relaxed);
        // Signal buffer to stop
    }

    pub fn check_stop(&self) -> Result<(), TrzszError> {
        if self.stop_and_delete.load(Ordering::Relaxed) {
            return Err(crate::comm::err_stopped_and_deleted());
        }
        if self.stopped.load(Ordering::Relaxed) {
            return Err(err_stopped());
        }
        Ok(())
    }

    pub fn get_new_timeout(&self) -> Option<Instant> {
        if self.transfer_config.timeout > 0 {
            Some(Instant::now() + Duration::from_secs(self.transfer_config.timeout as u64))
        } else {
            None
        }
    }

    pub fn send_line(&mut self, typ: &str, buf: &str) -> Result<(), TrzszError> {
        let line = format!("#{}:{}{}", typ, buf, self.transfer_config.newline);
        self.write_all(line.as_bytes())
    }

    pub fn write_all(&mut self, buf: &[u8]) -> Result<(), TrzszError> {
        write_all(&mut self.writer, buf).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })?;
        // Flush after every write to prevent the OS-level stdout buffer from
        // delaying transfer data. Without this, the receiver may wait forever
        // for chunks that are still sitting in the local stdout buffer.
        self.writer.flush().map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })
    }

    pub fn recv_line(&mut self, _expect_type: &str, may_has_junk: bool, timeout: Option<Instant>) -> Result<Vec<u8>, TrzszError> {
        self.check_stop()?;
        let line = self.buffer.read_line(may_has_junk, timeout)?;
        Ok(line)
    }

    pub fn recv_check(&mut self, expect_type: &str, may_has_junk: bool, timeout: Option<Instant>) -> Result<String, TrzszError> {
        let line = self.recv_line(expect_type, may_has_junk, timeout)?;
        let line_str = String::from_utf8_lossy(&line);
        let idx = line_str.find(':').ok_or_else(|| {
            TrzszError { message: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &line), err_type: "colon".to_string(), trace: true }
        })?;
        let typ = &line_str[1..idx];
        let buf = &line_str[idx + 1..];
        if typ != expect_type {
            return Err(TrzszError { message: buf.to_string(), err_type: typ.to_string(), trace: true });
        }
        Ok(buf.to_string())
    }

    pub fn send_integer(&mut self, typ: &str, val: i64) -> Result<(), TrzszError> {
        self.send_line(typ, &val.to_string())
    }

    pub fn recv_integer(&mut self, typ: &str, may_has_junk: bool, timeout: Option<Instant>) -> Result<i64, TrzszError> {
        let buf = self.recv_check(typ, may_has_junk, timeout)?;
        buf.parse::<i64>().map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })
    }

    pub fn check_integer(&mut self, expect: i64, timeout: Option<Instant>) -> Result<(), TrzszError> {
        let result = self.recv_integer("SUCC", false, timeout)?;
        if result != expect {
            return Err(TrzszError {
                message: format!("Integer check [{}] <> [{}]", result, expect),
                err_type: String::new(),
                trace: true,
            });
        }
        Ok(())
    }

    pub fn send_string(&mut self, typ: &str, str_val: &str) -> Result<(), TrzszError> {
        let encoded = escape::encode_string(str_val);
        self.send_line(typ, &encoded)
    }

    pub fn recv_string(&mut self, typ: &str, may_has_junk: bool, timeout: Option<Instant>) -> Result<String, TrzszError> {
        let buf = self.recv_check(typ, may_has_junk, timeout)?;
        let decoded = escape::decode_string(&buf)?;
        String::from_utf8(decoded).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })
    }

    pub fn send_binary(&mut self, typ: &str, data: &[u8]) -> Result<(), TrzszError> {
        let encoded = escape::encode_bytes(data);
        self.send_line(typ, &encoded)
    }

    pub fn recv_binary(&mut self, typ: &str, may_has_junk: bool, timeout: Option<Instant>) -> Result<Vec<u8>, TrzszError> {
        let buf = self.recv_check(typ, may_has_junk, timeout)?;
        escape::decode_string(&buf)
    }

    pub fn check_binary(&mut self, expect: &[u8], timeout: Option<Instant>) -> Result<(), TrzszError> {
        let result = self.recv_binary("SUCC", false, timeout)?;
        if result != expect {
            return Err(TrzszError {
                message: format!("Binary check [{}] <> [{}]",
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &result),
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, expect)),
                err_type: String::new(),
                trace: true,
            });
        }
        Ok(())
    }

    pub fn send_data(&mut self, data: &[u8]) -> Result<(), TrzszError> {
        self.check_stop()?;
        if !self.transfer_config.binary {
            return self.send_binary("DATA", data);
        }
        let escaped = escape::escape_data(data, &self.get_escape_table());
        let header = format!("#DATA:{}\n", escaped.len());
        self.write_all(header.as_bytes())?;
        self.write_all(&escaped)
    }

    pub fn recv_data(&mut self) -> Result<Vec<u8>, TrzszError> {
        let timeout = self.get_new_timeout();
        if !self.transfer_config.binary {
            return self.recv_binary("DATA", false, timeout);
        }
        let size = self.recv_integer("DATA", false, timeout)?;
        if size == 0 {
            return Ok(vec![]);
        }
        let data = self.buffer.read_binary(size as usize, timeout)?;
        let table = self.get_escape_table();
        let (unescaped, remaining) = escape::unescape_data(&data, &table, None)?;
        if !remaining.is_empty() {
            return Err(crate::comm::simple_error("Unescape has bytes remaining"));
        }
        Ok(unescaped)
    }

    fn get_escape_table(&self) -> EscapeTable {
        if let Some(ref chars) = self.transfer_config.escape_chars {
            if let Ok(arr) = chars.as_array().cloned().ok_or(()) {
                escape::escape_chars_to_table(&arr).unwrap_or_default()
            } else {
                EscapeTable::default()
            }
        } else {
            EscapeTable::default()
        }
    }

    pub fn send_action(&mut self, confirm: bool, server_version: Option<&TrzszVersion>, remote_is_windows: bool) -> Result<(), TrzszError> {
        let mut protocol = K_PROTOCOL_VERSION;
        if let Some(ver) = server_version {
            let v113 = TrzszVersion { major: 1, minor: 1, patch: 3 };
            let v110 = TrzszVersion { major: 1, minor: 0, patch: 0 };
            if ver.compare(&v113) <= 0 && ver.compare(&v110) >= 0 {
                protocol = 2;
            }
        }
        let action = TransferAction {
            lang: "rust".to_string(),
            version: TRZSZ_VERSION.to_string(),
            confirm,
            newline: "\n".to_string(),
            protocol,
            support_binary: true,
            support_directory: true,
            ..Default::default()
        };

        let act_str = serde_json::to_string(&action).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })?;

        if remote_is_windows {
            self.windows_protocol = true;
            self.transfer_config.newline = "!\n".to_string();
        }
        self.send_string("ACT", &act_str)
    }

    pub fn recv_action(&mut self) -> Result<TransferAction, TrzszError> {
        let act_str = self.recv_string("ACT", true, None)?;
        let mut action: TransferAction = serde_json::from_str(&act_str).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })?;
        if action.newline.is_empty() {
            action.newline = "\n".to_string();
        }
        self.transfer_config.newline = action.newline.clone();
        Ok(action)
    }

    pub fn send_config(&mut self, quiet: bool, binary: bool, directory: bool, overwrite: bool,
                        escape_chars: &serde_json::Value, tmux_pane_width: i32,
                        action: &TransferAction, compress: CompressType) -> Result<(), TrzszError> {
        let mut cfg_map = serde_json::json!({
            "lang": "rust",
        });
        if quiet {
            cfg_map["quiet"] = serde_json::json!(true);
        }
        if binary {
            cfg_map["binary"] = serde_json::json!(true);
            if !escape_chars.is_null() {
                cfg_map["escape_chars"] = escape_chars.clone();
            }
        }
        if directory {
            cfg_map["directory"] = serde_json::json!(true);
        }
        cfg_map["bufsize"] = serde_json::json!(self.transfer_config.bufsize);
        cfg_map["timeout"] = serde_json::json!(self.transfer_config.timeout);
        if overwrite {
            cfg_map["overwrite"] = serde_json::json!(true);
        }
        if tmux_pane_width > 0 {
            cfg_map["tmux_pane_width"] = serde_json::json!(tmux_pane_width);
        }
        if action.protocol > 0 {
            let proto = std::cmp::min(action.protocol, K_PROTOCOL_VERSION);
            cfg_map["protocol"] = serde_json::json!(proto);
        }
        if compress != CompressType::Auto {
            cfg_map["compress"] = serde_json::json!(compress as i32);
        }

        // Deserialize into TransferConfig
        self.transfer_config = serde_json::from_value(cfg_map.clone()).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })?;
        let cfg_str = serde_json::to_string(&cfg_map).unwrap_or_default();
        self.send_string("CFG", &cfg_str)
    }

    pub fn recv_config(&mut self) -> Result<TransferConfig, TrzszError> {
        let cfg_str = self.recv_string("CFG", true, self.get_new_timeout())?;
        let config: TransferConfig = serde_json::from_str(&cfg_str).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })?;
        self.transfer_config = config.clone();
        Ok(config)
    }

    pub fn client_exit(&mut self, msg: &str) -> Result<(), TrzszError> {
        self.send_string("EXIT", msg)
    }

    pub fn recv_exit(&mut self) -> Result<String, TrzszError> {
        self.recv_string("EXIT", false, self.get_new_timeout())
    }

    pub fn clean_input(&mut self, timeout_duration: Duration) {
        self.stopped.store(true, Ordering::SeqCst);
        self.buffer.drain_buffer();
        let start_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.last_input_time.store(start_ms, Ordering::SeqCst);
        loop {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let last = self.last_input_time.load(Ordering::SeqCst);
            let elapsed_since_last = now_ms - last;
            let remaining = timeout_duration.as_millis() as i64 - elapsed_since_last;
            if remaining <= 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(remaining as u64));
        }
    }

    pub fn server_exit(&mut self, msg: &str) {
        self.clean_input(Duration::from_millis(500));
        self.reset_term(msg, false);
    }

    fn reset_term(&self, msg: &str, ignorable: bool) {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut out = stdout.lock();

        // Already reset once: just print the green banner at the top of the
        // screen (if not ignorable), then return without touching the saved
        // cursor position again.
        if self.term_reseted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            if !ignorable {
                let normalized = msg.replace("\r\n", "\n").replace('\n', "\x1b[K\r\n");
                let banner = format!("\x1b[s\x1b[H\x1b[42;30m{}\x1b[K\x1b[0m\x1b[u", normalized);
                let _ = out.write_all(banner.as_bytes());
                let _ = out.flush();
            }
            return;
        }

        // First call: restore cursor to the saved position (set by the
        // initial "\x1b[s" in the trigger header), clear screen below it,
        // print the message, and re-show the cursor. Stdin is restored to
        // its pre-raw state via the RawModeGuard Drop in tsz_main/trz_main.
        if crate::comm::is_running_on_windows() {
            let normalized = msg.replace('\n', "\r\n");
            let _ = out.write_all(b"\x1b[H\x1b[2J\x1b[?1049l");
            let _ = out.write_all(normalized.as_bytes());
        } else {
            let _ = out.write_all(b"\x1b[u\x1b[0J");
            let _ = out.write_all(msg.as_bytes());
        }
        let _ = out.write_all(b"\r\n");
        crate::comm::show_cursor(&mut out);
        let _ = out.flush();
    }

    pub fn add_created_files(&mut self, path: &str) {
        self.created_files.push(path.to_string());
    }

    pub fn delete_created_files(&mut self) -> Vec<String> {
        let mut deleted = Vec::new();
        for path in &self.created_files {
            if Path::new(path).exists() {
                if fs::remove_dir_all(path).is_ok() {
                    deleted.push(path.clone());
                }
            }
        }
        deleted
    }

    pub fn client_error(&mut self, err: &TrzszError) {
        self.clean_input(self.clean_timeout);

        if self.stop_and_delete.load(Ordering::Relaxed) {
            let deleted = self.delete_created_files();
            if !deleted.is_empty() {
                let _ = self.send_string("fail", &crate::comm::join_file_names(&err.message, &deleted));
                return;
            }
        }

        let typ = if err.is_trace_back() { "FAIL" } else { "fail" };
        let _ = self.send_string(typ, &err.message);
    }

    pub fn server_error(&mut self, err: &TrzszError) {
        self.clean_input(self.clean_timeout);

        if err.is_stop_and_delete() {
            let deleted = self.delete_created_files();
            if !deleted.is_empty() {
                self.server_exit(&crate::comm::join_file_names(&err.message, &deleted));
                return;
            }
        }

        let typ = if err.is_trace_back() { "FAIL" } else { "fail" };
        let _ = self.send_string(typ, &err.message);
        self.server_exit(&err.message);
    }

    pub fn send_file_num(&mut self, num: i64) -> Result<(), TrzszError> {
        self.send_integer("NUM", num)?;
        self.check_integer(num, self.get_new_timeout())?;
        Ok(())
    }

    pub fn send_file_name(&mut self, src_file: &SourceFile) -> Result<(Option<Box<dyn FileReader>>, String), TrzszError> {
        let file_name = if self.transfer_config.directory {
            src_file.marshal().map_err(|e| TrzszError { message: e.to_string(), err_type: String::new(), trace: false })?
        } else {
            src_file.get_file_name().to_string()
        };
        self.send_string("NAME", &file_name)?;
        let remote_name = self.recv_string("SUCC", false, self.get_new_timeout())?;
        if src_file.is_dir {
            return Ok((None, remote_name));
        }
        let file = fs::File::open(&src_file.abs_path).map_err(|e| {
            TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
        })?;
        let reader: Box<dyn FileReader> = Box::new(SimpleFileReader { file, file_size: src_file.size });
        Ok((Some(reader), remote_name))
    }

    pub fn send_file_size(&mut self, size: i64) -> Result<(), TrzszError> {
        self.send_integer("SIZE", size)?;
        self.check_integer(size, self.get_new_timeout())?;
        Ok(())
    }

    pub fn send_file_data(&mut self, file: &mut dyn FileReader) -> Result<Vec<u8>, TrzszError> {
        let mut step: i64 = 0;
        let mut buf_size: usize = 1024;
        let mut buffer = vec![0u8; buf_size];
        let mut hasher = Md5::new();
        let size = file.size();

        while step < size {
            let begin_time = Instant::now();
            let m = size - step;
            let read_size = if (m as usize) < buf_size { m as usize } else { buf_size };
            let n = file.read(&mut buffer[..read_size]).map_err(|e| {
                TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
            })?;
            let length = n as i64;
            let data = &buffer[..n];
            self.send_data(data)?;
            hasher.update(data);
            self.check_integer(length, self.get_new_timeout())?;
            step += length;
            let chunk_time = begin_time.elapsed();
            if length == buf_size as i64 && chunk_time < Duration::from_millis(500) && buf_size < self.transfer_config.bufsize as usize {
                buf_size = (buf_size * 2).min(self.transfer_config.bufsize as usize);
                buffer.resize(buf_size, 0);
            } else if chunk_time >= Duration::from_secs(2) && buf_size > 1024 {
                buf_size = 1024;
                buffer.resize(buf_size, 0);
            }
            self.set_last_chunk_time(chunk_time);
        }
        Ok(hasher.finalize().to_vec())
    }

    pub fn send_file_md5(&mut self, digest: &[u8]) -> Result<(), TrzszError> {
        self.send_binary("MD5", digest)?;
        self.check_binary(digest, self.get_new_timeout())?;
        Ok(())
    }

    pub fn send_files(&mut self, source_files: &[SourceFile], progress: &mut Option<&mut dyn ProgressCallback>) -> Result<Vec<String>, TrzszError> {
        self.send_file_num(source_files.len() as i64)?;
        if let Some(ref mut p) = progress {
            p.on_num(source_files.len() as i64);
        }
        let mut remote_names = Vec::new();
        for src_file in source_files {
            let (file_opt, remote_name) = self.send_file_name(src_file)?;
            if let Some(ref mut p) = progress {
                p.on_name(src_file.get_file_name());
            }
            if !remote_names.contains(&remote_name) {
                remote_names.push(remote_name.clone());
            }
            if let Some(mut file) = file_opt {
                self.send_file_size(file.size())?;
                if let Some(ref mut p) = progress {
                    p.on_size(file.size());
                }
                let digest = self.send_file_data(&mut *file)?;
                self.send_file_md5(&digest)?;
                if let Some(ref mut p) = progress {
                    p.on_done();
                }
            }
        }
        Ok(remote_names)
    }

    pub fn recv_file_num(&mut self) -> Result<i64, TrzszError> {
        let num = self.recv_integer("NUM", false, self.get_new_timeout())?;
        self.send_integer("SUCC", num)?;
        Ok(num)
    }

    pub fn recv_file_name(&mut self, path: &Path) -> Result<(Option<Box<dyn FileWriter>>, String), TrzszError> {
        let file_name = self.recv_string("NAME", false, self.get_new_timeout())?;
        let (file, local_name) = if self.transfer_config.directory {
            let src_file: SourceFile = serde_json::from_str(&file_name).map_err(|e| {
                TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
            })?;
            let (f, ln) = self.create_dir_or_file(path, &src_file)?;
            (f, ln)
        } else {
            self.create_file(path, &file_name)?
        };
        self.send_string("SUCC", &local_name)?;
        Ok((file, local_name))
    }

    pub fn recv_file_size(&mut self) -> Result<i64, TrzszError> {
        let size = self.recv_integer("SIZE", false, self.get_new_timeout())?;
        self.send_integer("SUCC", size)?;
        Ok(size)
    }

    pub fn recv_file_data(&mut self, file: &mut dyn FileWriter, size: i64) -> Result<Vec<u8>, TrzszError> {
        let mut step: i64 = 0;
        let mut hasher = Md5::new();
        while step < size {
            let begin_time = Instant::now();
            let data = self.recv_data()?;
            file.write_all(&data).map_err(|e| {
                TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
            })?;
            let length = data.len() as i64;
            step += length;
            self.send_integer("SUCC", length)?;
            hasher.update(&data);
            self.set_last_chunk_time(begin_time.elapsed());
        }
        Ok(hasher.finalize().to_vec())
    }

    pub fn recv_file_md5(&mut self, digest: &[u8]) -> Result<(), TrzszError> {
        let expect_digest = self.recv_binary("MD5", false, self.get_new_timeout())?;
        if digest != expect_digest.as_slice() {
            return Err(crate::comm::simple_error("Check MD5 failed"));
        }
        self.send_binary("SUCC", digest)?;
        Ok(())
    }

    pub fn recv_files(&mut self, path: &Path, progress: &mut Option<&mut dyn ProgressCallback>) -> Result<Vec<String>, TrzszError> {
        let num = self.recv_file_num()?;
        if let Some(ref mut p) = progress {
            p.on_num(num);
        }
        let mut local_names = Vec::new();
        for _ in 0..num {
            let (file_opt, local_name) = self.recv_file_name(path)?;
            if let Some(ref mut p) = progress {
                p.on_name(&local_name);
            }
            if !local_names.contains(&local_name) {
                local_names.push(local_name.clone());
            }
            if let Some(mut file) = file_opt {
                let size = self.recv_file_size()?;
                if let Some(ref mut p) = progress {
                    p.on_size(size);
                }
                let digest = self.recv_file_data(&mut *file, size)?;
                self.recv_file_md5(&digest)?;
                if let Some(ref mut p) = progress {
                    p.on_done();
                }
            }
        }
        Ok(local_names)
    }

    fn create_file(&mut self, path: &Path, name: &str) -> Result<(Option<Box<dyn FileWriter>>, String), TrzszError> {
        let local_name = if self.transfer_config.overwrite {
            name.to_string()
        } else {
            get_new_name(path, name)?
        };
        let full_path = path.join(&local_name);
        let file = fs::File::create(&full_path).map_err(|e| {
            TrzszError { message: format!("Create file [{}] failed: {}", full_path.display(), e), err_type: String::new(), trace: false }
        })?;
        self.add_created_files(full_path.to_str().unwrap_or(""));
        Ok((Some(Box::new(SimpleFileWriter { file })), local_name))
    }

    fn create_dir_or_file(&mut self, path: &Path, src_file: &SourceFile) -> Result<(Option<Box<dyn FileWriter>>, String), TrzszError> {
        let local_name = if self.transfer_config.overwrite {
            src_file.rel_path[0].clone()
        } else {
            if let Some(v) = self.file_name_map.get(&src_file.path_id) {
                v.clone()
            } else {
                let name = get_new_name(path, &src_file.rel_path[0])?;
                self.file_name_map.insert(src_file.path_id, name.clone());
                name
            }
        };

        if src_file.is_dir {
            let full_path = if src_file.rel_path.len() > 1 {
                let parts: Vec<&str> = src_file.rel_path[1..].iter().map(|s| s.as_str()).collect();
                path.join(&local_name).join(parts.join("/"))
            } else {
                path.join(&local_name)
            };
            fs::create_dir_all(&full_path).map_err(|e| {
                TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
            })?;
            self.add_created_files(full_path.to_str().unwrap_or(""));
            return Ok((None, local_name));
        }

        let full_path = if src_file.rel_path.len() > 1 {
            let parts: Vec<&str> = src_file.rel_path[1..src_file.rel_path.len()-1].iter().map(|s| s.as_str()).collect();
            let dir = path.join(&local_name).join(parts.join("/"));
            fs::create_dir_all(&dir).map_err(|e| {
                TrzszError { message: e.to_string(), err_type: String::new(), trace: false }
            })?;
            dir.join(src_file.get_file_name())
        } else {
            path.join(&local_name)
        };

        let file = fs::File::create(&full_path).map_err(|e| {
            TrzszError { message: format!("Create file [{}] failed: {}", full_path.display(), e), err_type: String::new(), trace: false }
        })?;
        self.add_created_files(full_path.to_str().unwrap_or(""));
        Ok((Some(Box::new(SimpleFileWriter { file })), local_name))
    }

    fn set_last_chunk_time(&mut self, chunk_time: Duration) {
        let idx = self.last_chunk_time_idx.load(Ordering::Relaxed) as usize;
        self.last_chunk_time_arr[idx] = chunk_time;
        self.last_chunk_time_idx.store(((idx + 1) % K_LAST_CHUNK_TIME_COUNT) as u32, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_action_default() {
        let action = TransferAction::default();
        assert_eq!(action.lang, "go");
        assert!(action.confirm);
        assert_eq!(action.newline, "\n");
        assert!(action.support_binary);
    }

    #[test]
    fn test_transfer_config_default() {
        let config = TransferConfig::default();
        assert_eq!(config.timeout, 20);
        assert_eq!(config.newline, "\n");
        assert_eq!(config.bufsize, 10 * 1024 * 1024);
    }
}
