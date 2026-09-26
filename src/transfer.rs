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
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
#[cfg(not(target_has_atomic = "64"))]
use std::sync::Mutex;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use md5::Md5;
use sha2::Digest;

use crate::archive::{ArchiveFileReader, ArchiveFileWriter};
use crate::buffer::TrzszBuffer;
use crate::comm::{
    CompressType, FileReader, FileWriter, SimpleFileReader, SimpleFileWriter, SourceFile,
    TargetFile, TrzszError, err_stopped, get_new_name, write_all,
};
use crate::escape::{self, EscapeTable};
use crate::progress::ProgressCallback;
use crate::version::{TRZSZ_VERSION, TrzszVersion};

pub const K_PROTOCOL_VERSION2: i32 = 2;
pub const K_PROTOCOL_VERSION3: i32 = 3;
pub const K_PROTOCOL_VERSION4: i32 = 4;
// V4 adds directory archive streams; V3 adds prefix HASH resume and COMP negotiation.
pub const K_PROTOCOL_VERSION: i32 = K_PROTOCOL_VERSION4;

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

fn default_lang() -> String {
    "go".to_string()
}
fn default_true() -> bool {
    true
}
fn default_newline() -> String {
    "\n".to_string()
}

// ─── Transfer Config ───────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransferConfig {
    #[serde(default = "default_lang")]
    pub lang: String,
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
            lang: default_lang(),
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

fn default_timeout() -> i32 {
    20
}
fn default_bufsize() -> i64 {
    10 * 1024 * 1024
}

// ─── TrzszTransfer ─────────────────────────────────────────────────────────

pub struct TrzszTransfer {
    pub buffer: TrzszBuffer,
    pub writer: Box<dyn Write + Send>,
    pub stopped: AtomicBool,
    pub stop_and_delete: Arc<AtomicBool>,
    pub term_reseted: AtomicBool,
    pub(crate) pausing: Arc<AtomicBool>,
    pub(crate) pause_idx: Arc<AtomicU32>,
    pause_supported: Arc<AtomicBool>,
    pub clean_timeout: Duration,
    #[cfg(target_has_atomic = "64")]
    pub last_input_time: AtomicI64,
    #[cfg(not(target_has_atomic = "64"))]
    pub last_input_time: Mutex<i64>,
    pub last_chunk_time_arr: [Duration; K_LAST_CHUNK_TIME_COUNT],
    pub last_chunk_time_idx: AtomicU32,
    pub stdin_state: Option<()>,
    pub file_name_map: HashMap<i32, String>,
    pub windows_protocol: bool,
    pub flush_in_time: bool,
    pub transfer_config: TransferConfig,
    pub peer_lang: String,
    pub created_files: Vec<String>,
    pub tunnel_connected: bool,
    tunnel_tx: mpsc::SyncSender<std::net::TcpStream>,
    tunnel_rx: mpsc::Receiver<std::net::TcpStream>,
    pub bg_chan: mpsc::SyncSender<()>,
}

impl TrzszTransfer {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        let (bg_tx, _) = mpsc::sync_channel(1);
        let (tunnel_tx, tunnel_rx) = mpsc::sync_channel(1);
        TrzszTransfer {
            buffer: TrzszBuffer::new(),
            writer,
            stopped: AtomicBool::new(false),
            stop_and_delete: Arc::new(AtomicBool::new(false)),
            term_reseted: AtomicBool::new(false),
            pausing: Arc::new(AtomicBool::new(false)),
            pause_idx: Arc::new(AtomicU32::new(0)),
            pause_supported: Arc::new(AtomicBool::new(false)),
            clean_timeout: Duration::from_millis(100),
            #[cfg(target_has_atomic = "64")]
            last_input_time: AtomicI64::new(0),
            #[cfg(not(target_has_atomic = "64"))]
            last_input_time: Mutex::new(0),
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
            peer_lang: String::new(),
            tunnel_connected: false,
            tunnel_tx,
            tunnel_rx,
            bg_chan: bg_tx,
        }
    }

    pub fn background(&self) -> mpsc::Receiver<()> {
        let _ = self.bg_chan.clone(); // keep sender alive
        let (_, rx) = mpsc::sync_channel(1);
        rx
    }

    pub(crate) fn accept_on_tunnel(
        &mut self,
        listener: std::net::TcpListener,
        unique_id: String,
        port: i32,
    ) {
        let tx = self.tunnel_tx.clone();
        let input = self.buffer.sender();
        std::thread::spawn(move || {
            let uid = unique_id.strip_suffix("00").unwrap_or(&unique_id);
            let client_hello = format!("::TRZSZ::CLIENT::HELLO::{}:{}", uid, port);
            let server_hello = format!("::TRZSZ::SERVER::HELLO::{}:{}", uid, port);
            let (stream, mut reader) = loop {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let mut hello = vec![0; client_hello.len()];
                if std::io::Read::read_exact(&mut stream, &mut hello).is_err()
                    || hello != client_hello.as_bytes()
                    || std::io::Write::write_all(&mut stream, server_hello.as_bytes()).is_err()
                {
                    continue;
                }
                let Ok(reader) = stream.try_clone() else {
                    continue;
                };
                break (stream, reader);
            };
            if tx.send(stream).is_err() {
                return;
            }
            let mut buf = [0u8; 32 * 1024];
            loop {
                match std::io::Read::read(&mut reader, &mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if input.send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    fn switch_to_tunnel(&mut self) -> Result<(), TrzszError> {
        let stream = self
            .tunnel_rx
            .try_recv()
            .map_err(|_| crate::comm::simple_error("Tunnel connection was not established"))?;
        let writer = stream
            .try_clone()
            .map_err(|e| crate::comm::simple_trzsz_error("Clone tunnel connection failed", e))?;
        self.writer = Box::new(writer);
        self.tunnel_connected = true;
        Ok(())
    }

    pub fn add_received_data(&self, buf: &[u8], _tunnel: bool) {
        self.buffer.add_buffer(buf);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        #[cfg(target_has_atomic = "64")]
        self.last_input_time.store(now, Ordering::Relaxed);
        #[cfg(not(target_has_atomic = "64"))]
        {
            *self.last_input_time.lock().unwrap() = now;
        }
    }

    pub fn stop_transferring_files(&self, stop_and_delete: bool) {
        self.stop_and_delete
            .store(stop_and_delete, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);
        self.buffer.stop();
    }
    pub(crate) fn pause_handles(&self) -> (Arc<AtomicBool>, Arc<AtomicU32>, Arc<AtomicBool>) {
        (
            self.pausing.clone(),
            self.pause_idx.clone(),
            self.pause_supported.clone(),
        )
    }
    pub(crate) fn stop_and_delete_handle(&self) -> Arc<AtomicBool> {
        self.stop_and_delete.clone()
    }

    pub(crate) fn stop_handle(&self) -> std::sync::Arc<AtomicBool> {
        self.buffer.stop_handle()
    }

    pub fn check_stop(&self) -> Result<(), TrzszError> {
        if self.stop_and_delete.load(Ordering::SeqCst) {
            return Err(crate::comm::err_stopped_and_deleted());
        }
        if self.stopped.load(Ordering::SeqCst) || self.buffer.is_stopped() {
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
        write_all(&mut self.writer, buf).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        // Flush after every write to prevent the OS-level stdout buffer from
        // delaying transfer data. Without this, the receiver may wait forever
        // for chunks that are still sitting in the local stdout buffer.
        self.writer.flush().map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })
    }

    pub fn recv_line(
        &mut self,
        expect_type: &str,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<Vec<u8>, TrzszError> {
        self.check_stop()?;
        let may_has_junk = may_has_junk || self.transfer_config.tmux_output_junk;
        self.buffer
            .read_line_for(Some(expect_type), may_has_junk, timeout)
    }

    pub fn recv_check(
        &mut self,
        expect_type: &str,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<String, TrzszError> {
        let v3 = self.transfer_config.protocol >= 3;
        let mut next_timeout = timeout;
        loop {
            while v3 && self.pausing.load(Ordering::SeqCst) {
                self.check_stop()?;
                std::thread::sleep(Duration::from_millis(100));
                next_timeout = self.get_new_timeout();
            }
            let pause_idx = self.pause_idx.load(Ordering::SeqCst);
            let line = match self.recv_line(expect_type, may_has_junk, next_timeout) {
                Ok(line) => line,
                Err(error)
                    if v3
                        && error.message == crate::comm::ERR_RECEIVE_DATA_TIMEOUT.message
                        && pause_idx < self.pause_idx.load(Ordering::SeqCst) =>
                {
                    next_timeout = self.get_new_timeout();
                    continue;
                }
                Err(error) => return Err(error),
            };
            let idx = line.iter().position(|&b| b == b':').filter(|&idx| idx >= 1);
            let idx = idx.ok_or_else(|| {
                crate::comm::new_trzsz_error(&escape::encode_bytes(&line), "colon", true)
            })?;
            let typ = String::from_utf8_lossy(&line[1..idx]).into_owned();
            let buf = String::from_utf8_lossy(&line[idx + 1..]).into_owned();
            if v3 && buf == "=" {
                next_timeout = self.get_new_timeout();
                continue;
            }
            if typ != expect_type {
                return Err(crate::comm::new_trzsz_error(&buf, &typ, true));
            }
            return Ok(buf);
        }
    }
    pub(crate) fn recv_check_limited(
        &mut self,
        expect_type: &str,
        may_has_junk: bool,
        timeout: Option<Instant>,
        max_line_size: usize,
    ) -> Result<String, TrzszError> {
        let v3 = self.transfer_config.protocol >= 3;
        let mut next_timeout = timeout;
        loop {
            self.check_stop()?;
            while v3 && self.pausing.load(Ordering::SeqCst) {
                self.check_stop()?;
                std::thread::sleep(Duration::from_millis(100));
                next_timeout = self.get_new_timeout();
            }
            let pause_idx = self.pause_idx.load(Ordering::SeqCst);
            let line = match self.buffer.read_line_for_limited(
                Some(expect_type),
                may_has_junk || self.transfer_config.tmux_output_junk,
                next_timeout,
                max_line_size,
            ) {
                Ok(line) => line,
                Err(error)
                    if v3
                        && error.message == crate::comm::ERR_RECEIVE_DATA_TIMEOUT.message
                        && pause_idx < self.pause_idx.load(Ordering::SeqCst) =>
                {
                    next_timeout = self.get_new_timeout();
                    continue;
                }
                Err(error) => return Err(error),
            };
            let idx = line.iter().position(|&b| b == b':').filter(|&idx| idx >= 1);
            let idx = idx.ok_or_else(|| {
                crate::comm::new_trzsz_error(&escape::encode_bytes(&line), "colon", true)
            })?;
            let typ = String::from_utf8_lossy(&line[1..idx]).into_owned();
            let payload = String::from_utf8_lossy(&line[idx + 1..]).into_owned();
            if v3 && payload == "=" {
                next_timeout = self.get_new_timeout();
                continue;
            }
            if typ != expect_type {
                return Err(crate::comm::new_trzsz_error(&payload, &typ, true));
            }
            return Ok(payload);
        }
    }

    pub fn send_integer(&mut self, typ: &str, val: i64) -> Result<(), TrzszError> {
        self.send_line(typ, &val.to_string())
    }

    pub fn recv_integer(
        &mut self,
        typ: &str,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<i64, TrzszError> {
        let buf = self.recv_check(typ, may_has_junk, timeout)?;
        buf.parse::<i64>().map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })
    }

    pub fn check_integer(
        &mut self,
        expect: i64,
        timeout: Option<Instant>,
    ) -> Result<(), TrzszError> {
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

    pub fn recv_string(
        &mut self,
        typ: &str,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<String, TrzszError> {
        let buf = self.recv_check(typ, may_has_junk, timeout)?;
        let decoded = escape::decode_string(&buf)?;
        String::from_utf8(decoded).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })
    }

    pub fn send_binary(&mut self, typ: &str, data: &[u8]) -> Result<(), TrzszError> {
        let encoded = escape::encode_bytes(data);
        self.send_line(typ, &encoded)
    }

    pub fn recv_binary(
        &mut self,
        typ: &str,
        may_has_junk: bool,
        timeout: Option<Instant>,
    ) -> Result<Vec<u8>, TrzszError> {
        let buf = self.recv_check(typ, may_has_junk, timeout)?;
        escape::decode_string(&buf)
    }

    pub fn check_binary(
        &mut self,
        expect: &[u8],
        timeout: Option<Instant>,
    ) -> Result<(), TrzszError> {
        let result = self.recv_binary("SUCC", false, timeout)?;
        if result != expect {
            return Err(TrzszError {
                message: format!(
                    "Binary check [{}] <> [{}]",
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &result),
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, expect)
                ),
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
        let table = self.get_escape_table()?;
        let escaped = escape::escape_data(data, &table);
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
        if size < 0 {
            return Err(crate::comm::simple_error("Invalid DATA size"));
        }
        if size == 0 {
            return Ok(vec![]);
        }
        let data = self.buffer.read_binary(size as usize, timeout)?;
        let table = self.get_escape_table()?;
        let (unescaped, remaining) = escape::unescape_data(&data, &table, None)?;
        if !remaining.is_empty() {
            return Err(crate::comm::simple_error("Unescape has bytes remaining"));
        }
        Ok(unescaped)
    }

    fn get_escape_table(&self) -> Result<EscapeTable, TrzszError> {
        if let Some(chars) = &self.transfer_config.escape_chars {
            let arr = chars
                .as_array()
                .ok_or_else(|| crate::comm::simple_error("Escape chars invalid"))?;
            escape::escape_chars_to_table(arr)
        } else {
            Ok(EscapeTable::default())
        }
    }

    pub fn send_action(
        &mut self,
        confirm: bool,
        server_version: Option<&TrzszVersion>,
        remote_is_windows: bool,
    ) -> Result<(), TrzszError> {
        let mut protocol = K_PROTOCOL_VERSION;
        if let Some(ver) = server_version {
            let v113 = TrzszVersion {
                major: 1,
                minor: 1,
                patch: 3,
            };
            let v110 = TrzszVersion {
                major: 1,
                minor: 0,
                patch: 0,
            };
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

        let act_str = serde_json::to_string(&action).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;

        if remote_is_windows {
            self.windows_protocol = true;
            self.transfer_config.newline = "!\n".to_string();
        }
        self.send_string("ACT", &act_str)
    }

    pub fn recv_action(&mut self) -> Result<TransferAction, TrzszError> {
        let act_str = self.recv_string("ACT", true, None)?;
        let mut action: TransferAction =
            serde_json::from_str(&act_str).map_err(|e| TrzszError {
                message: e.to_string(),
                err_type: String::new(),
                trace: false,
            })?;
        if action.newline.is_empty() {
            action.newline = "\n".to_string();
        }
        self.transfer_config.newline = action.newline.clone();
        self.peer_lang = action.lang.clone();
        if action.tunnel {
            self.switch_to_tunnel()?;
        }
        Ok(action)
    }

    pub fn send_config(
        &mut self,
        quiet: bool,
        binary: bool,
        directory: bool,
        overwrite: bool,
        escape_chars: &serde_json::Value,
        tmux_pane_width: i32,
        action: &TransferAction,
        compress: CompressType,
    ) -> Result<(), TrzszError> {
        self.peer_lang = action.lang.clone();
        let mut cfg_map = serde_json::json!({
            "lang": "rust",
        });
        if quiet {
            cfg_map["quiet"] = serde_json::json!(true);
        }
        if action.tunnel {
            cfg_map["binary"] = serde_json::json!(true);
            if self.transfer_config.fork {
                cfg_map["fork"] = serde_json::json!(true);
                cfg_map["quiet"] = serde_json::json!(true);
            }
        }
        if binary {
            cfg_map["binary"] = serde_json::json!(true);
            if !action.tunnel && !escape_chars.is_null() {
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
        if self.transfer_config.tmux_output_junk {
            cfg_map["tmux_output_junk"] = serde_json::json!(true);
        }

        // Deserialize into TransferConfig
        self.transfer_config = serde_json::from_value(cfg_map.clone()).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        self.pause_supported
            .store(self.transfer_config.protocol >= 3, Ordering::SeqCst);
        let cfg_str = serde_json::to_string(&cfg_map).unwrap_or_default();
        self.send_string("CFG", &cfg_str)
    }

    pub fn recv_config(&mut self) -> Result<TransferConfig, TrzszError> {
        let cfg_str = self.recv_string("CFG", true, self.get_new_timeout())?;
        let config: TransferConfig = serde_json::from_str(&cfg_str).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        self.peer_lang = config.lang.clone();
        self.transfer_config = config.clone();
        self.pause_supported
            .store(config.protocol >= 3, Ordering::SeqCst);
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
        #[cfg(target_has_atomic = "64")]
        self.last_input_time.store(start_ms, Ordering::SeqCst);
        #[cfg(not(target_has_atomic = "64"))]
        {
            *self.last_input_time.lock().unwrap() = start_ms;
        }
        loop {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            #[cfg(target_has_atomic = "64")]
            let last = self.last_input_time.load(Ordering::SeqCst);
            #[cfg(not(target_has_atomic = "64"))]
            let last = *self.last_input_time.lock().unwrap();
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
        if self
            .term_reseted
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
            let path = Path::new(path);
            let removed = match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_dir() => fs::remove_dir_all(path),
                Ok(_) => fs::remove_file(path),
                Err(_) => continue,
            };
            if removed.is_ok() {
                deleted.push(path.to_string_lossy().into_owned());
            }
        }
        deleted
    }

    pub fn client_error(&mut self, err: &TrzszError) {
        self.clean_input(self.clean_timeout);

        if self.stop_and_delete.load(Ordering::Relaxed) {
            let deleted = self.delete_created_files();
            if !deleted.is_empty() {
                let _ = self.send_string(
                    "fail",
                    &crate::comm::join_file_names(&err.message, &deleted),
                );
                return;
            }
        }

        let typ = if err.is_trace_back() { "FAIL" } else { "fail" };
        let _ = self.send_string(typ, &err.message);
    }

    pub fn server_error(&mut self, err: &TrzszError) {
        self.clean_input(self.clean_timeout);

        if self.stop_and_delete.load(Ordering::SeqCst) || err.is_stop_and_delete() {
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

    pub fn send_file_name(
        &mut self,
        src_file: &SourceFile,
    ) -> Result<(Option<Box<dyn FileReader>>, String), TrzszError> {
        let file_name = if self.transfer_config.directory {
            src_file.marshal().map_err(|e| TrzszError {
                message: e.to_string(),
                err_type: String::new(),
                trace: false,
            })?
        } else {
            src_file.get_file_name().to_string()
        };
        self.send_string("NAME", &file_name)?;
        let remote_name = self.recv_string("SUCC", false, self.get_new_timeout())?;
        if src_file.is_dir {
            return Ok((None, remote_name));
        }
        let file = fs::File::open(&src_file.abs_path).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        let reader: Box<dyn FileReader> = Box::new(SimpleFileReader {
            file,
            file_size: src_file.size,
        });
        Ok((Some(reader), remote_name))
    }

    fn send_file_name_v3(
        &mut self,
        src_file: &SourceFile,
        progress: &mut Option<&mut dyn ProgressCallback>,
    ) -> Result<(Option<Box<dyn FileReader>>, String), TrzszError> {
        let source = src_file
            .marshal()
            .map_err(|e| crate::comm::simple_trzsz_error("Marshal source file failed", e))?;
        self.send_string("NAME", &source)?;
        let payload = self.recv_string("SUCC", false, self.get_new_timeout())?;
        let target: TargetFile = serde_json::from_str(&payload)
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid target file", e))?;
        if target.size < 0 {
            return Err(crate::comm::simple_error("Invalid target file size"));
        }
        if !src_file.sub_files.is_empty() {
            let reader = ArchiveFileReader::new(src_file.sub_files.clone(), src_file.path_id)
                .map_err(|error| {
                    crate::comm::simple_trzsz_error("Create archive reader failed", error)
                })?;
            return Ok((Some(Box::new(reader)), target.name));
        }
        if src_file.is_dir {
            return Ok((None, target.name));
        }

        let file = fs::File::open(&src_file.abs_path).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        let mut reader = SimpleFileReader {
            file,
            file_size: src_file.size,
        };
        if target.size > 0 {
            reader.file_size = crate::v3::send_prefix_hash(
                self,
                &mut reader,
                src_file.size,
                target.size,
                progress,
            )?;
        }
        Ok((Some(Box::new(reader)), target.name))
    }

    pub fn send_file_size(&mut self, size: i64) -> Result<(), TrzszError> {
        self.send_integer("SIZE", size)?;
        self.check_integer(size, self.get_new_timeout())?;
        Ok(())
    }

    pub fn send_file_data(&mut self, file: &mut dyn FileReader) -> Result<Vec<u8>, TrzszError> {
        self.send_file_data_with_progress(file, &mut None)
    }

    fn send_file_data_with_progress(
        &mut self,
        file: &mut dyn FileReader,
        progress: &mut Option<&mut dyn ProgressCallback>,
    ) -> Result<Vec<u8>, TrzszError> {
        if self.transfer_config.protocol >= K_PROTOCOL_VERSION2 {
            return crate::v2::send_file_data(self, file, progress);
        }

        let mut step: i64 = 0;
        let mut buf_size: usize = 1024;
        let mut buffer = vec![0u8; buf_size];
        let mut hasher = Md5::new();
        let size = file.size();
        if size < 0 {
            return Err(crate::comm::simple_error("Invalid file size"));
        }

        while step < size {
            let begin_time = Instant::now();
            let m = size - step;
            let read_size = if (m as usize) < buf_size {
                m as usize
            } else {
                buf_size
            };
            let n = file
                .read(&mut buffer[..read_size])
                .map_err(|e| TrzszError {
                    message: e.to_string(),
                    err_type: String::new(),
                    trace: false,
                })?;
            if n == 0 {
                return Err(crate::comm::simple_trzsz_error(
                    "Unexpected EOF",
                    format!("sent {} of {} bytes", step, size),
                ));
            }
            let length = n as i64;
            let data = &buffer[..n];
            self.send_data(data)?;
            hasher.update(data);
            self.check_integer(length, self.get_new_timeout())?;
            step += length;
            if let Some(p) = progress.as_mut() {
                p.on_step(step);
            }
            let chunk_time = begin_time.elapsed();
            if length == buf_size as i64
                && chunk_time < Duration::from_millis(500)
                && buf_size < self.transfer_config.bufsize as usize
            {
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

    pub fn send_files(
        &mut self,
        source_files: &[SourceFile],
        progress: &mut Option<&mut dyn ProgressCallback>,
    ) -> Result<Vec<String>, TrzszError> {
        let source_files = prepare_source_files(
            source_files,
            self.transfer_config.protocol,
            self.transfer_config.overwrite,
        );
        self.send_file_num(source_files.len() as i64)?;
        if let Some(p) = progress.as_mut() {
            p.on_num(source_files.len() as i64);
        }
        let mut remote_names = Vec::new();
        for src_file in &source_files {
            let (file_opt, remote_name) = if self.transfer_config.protocol >= K_PROTOCOL_VERSION3 {
                self.send_file_name_v3(src_file, progress)?
            } else {
                self.send_file_name(src_file)?
            };
            if let Some(p) = progress.as_mut() {
                p.on_name(src_file.get_file_name());
            }
            if !remote_names.contains(&remote_name) {
                remote_names.push(remote_name.clone());
            }
            if let Some(mut file) = file_opt {
                with_closed_file_reader(&mut *file, |file| {
                    self.send_file_size(file.size())?;
                    if let Some(p) = progress.as_mut() {
                        p.on_size(file.size());
                    }
                    let digest = self.send_file_data_with_progress(file, progress)?;
                    self.send_file_md5(&digest)?;
                    if let Some(p) = progress.as_mut() {
                        p.on_done();
                    }
                    Ok(())
                })?;
            }
        }
        Ok(remote_names)
    }

    pub fn recv_file_num(&mut self) -> Result<i64, TrzszError> {
        let num = self.recv_integer("NUM", false, self.get_new_timeout())?;
        self.send_integer("SUCC", num)?;
        Ok(num)
    }

    pub fn recv_file_name(
        &mut self,
        path: &Path,
    ) -> Result<(Option<Box<dyn FileWriter>>, String), TrzszError> {
        let (file, local_name, _) = self.recv_file_name_with_source_name(path)?;
        Ok((file, local_name))
    }

    fn recv_file_name_with_source_name(
        &mut self,
        path: &Path,
    ) -> Result<(Option<Box<dyn FileWriter>>, String, String), TrzszError> {
        let file_name = self.recv_string("NAME", false, self.get_new_timeout())?;
        let (mut file, local_name, source_name) = if self.transfer_config.directory {
            let src_file: SourceFile =
                serde_json::from_str(&file_name).map_err(|e| TrzszError {
                    message: e.to_string(),
                    err_type: String::new(),
                    trace: false,
                })?;
            let source_name = src_file.get_file_name().to_string();
            let (f, ln) = self.create_dir_or_file(path, &src_file, true)?;
            (f, ln, source_name)
        } else {
            let (f, ln) = self.create_file(path, &file_name)?;
            (f, ln, file_name)
        };
        if let Err(error) = self.send_string("SUCC", &local_name) {
            if let Some(file) = file.as_mut() {
                let _ = close_file_writer(&mut **file);
            }
            return Err(error);
        }
        Ok((file, local_name, source_name))
    }

    fn recv_file_name_v3(
        &mut self,
        path: &Path,
        progress: &mut Option<&mut dyn ProgressCallback>,
    ) -> Result<(Option<Box<dyn FileWriter>>, String, String), TrzszError> {
        let payload = self.recv_string("NAME", false, self.get_new_timeout())?;
        let source: SourceFile = serde_json::from_str(&payload)
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid source file", e))?;
        if source.size < 0 {
            return Err(crate::comm::simple_error("Invalid source file size"));
        }
        let source_name = source.get_file_name().to_string();
        let (mut file, local_name) = self.create_dir_or_file(path, &source, false)?;
        let target_size =
            match file.as_ref() {
                Some(writer) => i64::try_from(writer.size().map_err(|e| {
                    crate::comm::simple_trzsz_error("Get target file size failed", e)
                })?)
                .map_err(|e| crate::comm::simple_trzsz_error("Invalid target file size", e))?,
                None => 0,
            };
        let target = TargetFile {
            name: local_name.clone(),
            size: target_size,
        };
        let target = serde_json::to_string(&target)
            .map_err(|e| crate::comm::simple_trzsz_error("Marshal target file failed", e))?;
        if let Err(error) = self.send_string("SUCC", &target) {
            if let Some(writer) = file.as_mut() {
                let _ = close_file_writer(&mut **writer);
            }
            return Err(error);
        }
        if target_size > 0 {
            if let Some(writer) = file.as_mut() {
                if let Err(error) =
                    crate::v3::recv_prefix_hash(self, &mut **writer, source.size, progress)
                {
                    let _ = close_file_writer(&mut **writer);
                    return Err(error);
                }
            }
        }
        Ok((file, local_name, source_name))
    }

    pub fn recv_file_size(&mut self) -> Result<i64, TrzszError> {
        let size = self.recv_integer("SIZE", false, self.get_new_timeout())?;
        if size < 0 {
            return Err(crate::comm::simple_error("Invalid file size"));
        }
        self.send_integer("SUCC", size)?;
        Ok(size)
    }

    pub fn recv_file_data(
        &mut self,
        file: &mut dyn FileWriter,
        size: i64,
    ) -> Result<Vec<u8>, TrzszError> {
        self.recv_file_data_with_progress(file, size, &mut None)
    }

    fn recv_file_data_with_progress(
        &mut self,
        file: &mut dyn FileWriter,
        size: i64,
        progress: &mut Option<&mut dyn ProgressCallback>,
    ) -> Result<Vec<u8>, TrzszError> {
        if size < 0 {
            return Err(crate::comm::simple_error("Invalid file size"));
        }
        if self.transfer_config.protocol >= K_PROTOCOL_VERSION2 {
            return crate::v2::recv_file_data(self, file, size, progress);
        }
        let mut step: i64 = 0;
        let mut hasher = Md5::new();
        while step < size {
            let begin_time = Instant::now();
            let data = self.recv_data()?;
            let length = data.len() as i64;
            if length == 0 {
                return Err(crate::comm::simple_trzsz_error(
                    "Unexpected empty DATA chunk",
                    format!("at {} of {} bytes", step, size),
                ));
            }
            if length > size - step {
                return Err(crate::comm::simple_error(
                    "DATA exceeds negotiated file size",
                ));
            }
            file.write_all(&data).map_err(|e| TrzszError {
                message: e.to_string(),
                err_type: String::new(),
                trace: false,
            })?;
            step += length;
            self.send_integer("SUCC", length)?;
            hasher.update(&data);
            if let Some(p) = progress.as_mut() {
                p.on_step(step);
            }
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

    pub fn recv_files(
        &mut self,
        path: &Path,
        progress: &mut Option<&mut dyn ProgressCallback>,
    ) -> Result<Vec<String>, TrzszError> {
        let num = self.recv_file_num()?;
        if let Some(p) = progress.as_mut() {
            p.on_num(num);
        }
        let mut local_names = Vec::new();
        for _ in 0..num {
            let (file_opt, local_name, source_name) =
                if self.transfer_config.protocol >= K_PROTOCOL_VERSION3 {
                    self.recv_file_name_v3(path, progress)?
                } else {
                    self.recv_file_name_with_source_name(path)?
                };
            if let Some(p) = progress.as_mut() {
                p.on_name(&source_name);
            }
            if !local_names.contains(&local_name) {
                local_names.push(local_name.clone());
            }
            if let Some(mut file) = file_opt {
                with_closed_file_writer(&mut *file, |file| {
                    let size = self.recv_file_size()?;
                    if let Some(p) = progress.as_mut() {
                        p.on_size(size);
                    }
                    let digest = self.recv_file_data_with_progress(file, size, progress)?;
                    self.recv_file_md5(&digest)?;
                    if let Some(p) = progress.as_mut() {
                        p.on_done();
                    }
                    Ok(())
                })?;
            }
        }
        Ok(local_names)
    }

    fn create_file(
        &mut self,
        path: &Path,
        name: &str,
    ) -> Result<(Option<Box<dyn FileWriter>>, String), TrzszError> {
        let local_name = if self.transfer_config.overwrite {
            name.to_string()
        } else {
            get_new_name(path, name)?
        };
        let full_path = path.join(&local_name);
        let file = fs::File::create(&full_path).map_err(|e| file_creation_error(&full_path, e))?;
        self.add_created_files(full_path.to_str().unwrap_or(""));
        Ok((Some(Box::new(SimpleFileWriter { file })), local_name))
    }

    fn create_dir_or_file(
        &mut self,
        path: &Path,
        src_file: &SourceFile,
        truncate: bool,
    ) -> Result<(Option<Box<dyn FileWriter>>, String), TrzszError> {
        if src_file.rel_path.is_empty() {
            return Err(crate::comm::simple_error(
                "Invalid source file: empty path_name",
            ));
        }
        if src_file.archive {
            crate::archive::validate_archive_root(&src_file.rel_path[0]).map_err(|error| {
                crate::comm::simple_trzsz_error("Invalid archive root path", error)
            })?;
        }
        let local_name = if self.transfer_config.overwrite {
            src_file.rel_path[0].clone()
        } else if let Some(v) = self.file_name_map.get(&src_file.path_id) {
            v.clone()
        } else {
            let name = get_new_name(path, &src_file.rel_path[0])?;
            self.file_name_map.insert(src_file.path_id, name.clone());
            name
        };

        if src_file.archive {
            if !src_file.is_dir {
                return Err(crate::comm::simple_error("Archive is not a directory"));
            }
            let root = path.join(&local_name);
            create_dir_all_with_mode(&root, src_file.perm.unwrap_or(0) | 0o700).map_err(
                |error| crate::comm::simple_trzsz_error("Create archive directory failed", error),
            )?;
            self.add_created_files(root.to_str().unwrap_or(""));
            let writer = ArchiveFileWriter::new(root, src_file.path_id);
            return Ok((Some(Box::new(writer)), local_name));
        }
        if src_file.is_dir {
            let full_path = if src_file.rel_path.len() > 1 {
                let parts: Vec<&str> = src_file.rel_path[1..].iter().map(|s| s.as_str()).collect();
                path.join(&local_name).join(parts.join("/"))
            } else {
                path.join(&local_name)
            };
            create_dir_all_with_mode(&full_path, src_file.perm.unwrap_or(0) | 0o700).map_err(
                |e| TrzszError {
                    message: e.to_string(),
                    err_type: String::new(),
                    trace: false,
                },
            )?;
            self.add_created_files(full_path.to_str().unwrap_or(""));
            return Ok((None, local_name));
        }

        let full_path = if src_file.rel_path.len() > 1 {
            let parts: Vec<&str> = src_file.rel_path[1..src_file.rel_path.len() - 1]
                .iter()
                .map(|s| s.as_str())
                .collect();
            let dir = path.join(&local_name).join(parts.join("/"));
            create_dir_all_with_mode(&dir, 0o700).map_err(|e| TrzszError {
                message: e.to_string(),
                err_type: String::new(),
                trace: false,
            })?;
            dir.join(src_file.get_file_name())
        } else {
            path.join(&local_name)
        };

        let file = create_file_with_mode(&full_path, src_file.perm.unwrap_or(0) | 0o600, truncate)
            .map_err(|e| file_creation_error(&full_path, e))?;
        self.add_created_files(full_path.to_str().unwrap_or(""));
        Ok((Some(Box::new(SimpleFileWriter { file })), local_name))
    }

    fn set_last_chunk_time(&mut self, chunk_time: Duration) {
        let idx = self.last_chunk_time_idx.load(Ordering::Relaxed) as usize;
        self.last_chunk_time_arr[idx] = chunk_time;
        self.last_chunk_time_idx.store(
            ((idx + 1) % K_LAST_CHUNK_TIME_COUNT) as u32,
            Ordering::Relaxed,
        );
    }
}

fn archive_source_files(source_files: &[SourceFile]) -> Vec<SourceFile> {
    let mut archived = Vec::<SourceFile>::new();
    let mut positions = HashMap::<i32, usize>::new();
    for source in source_files {
        if let Some(index) = positions.get(&source.path_id) {
            archived[*index].sub_files.push(source.clone());
        } else {
            positions.insert(source.path_id, archived.len());
            archived.push(source.clone());
        }
    }
    archived
}

fn prepare_source_files(
    source_files: &[SourceFile],
    protocol: i32,
    overwrite: bool,
) -> Vec<SourceFile> {
    if protocol >= K_PROTOCOL_VERSION4 && !overwrite {
        archive_source_files(source_files)
    } else {
        source_files.to_vec()
    }
}

fn file_creation_error(path: &Path, error: io::Error) -> TrzszError {
    let message = match error.kind() {
        io::ErrorKind::PermissionDenied => {
            format!("No permission to write: {}", path.display())
        }
        io::ErrorKind::IsADirectory => format!("Is a directory: {}", path.display()),
        io::ErrorKind::NotADirectory => format!("Not a directory: {}", path.display()),
        _ => format!("Create file [{}] failed: {}", path.display(), error),
    };
    TrzszError {
        message,
        err_type: String::new(),
        trace: false,
    }
}

fn create_file_with_mode(path: &Path, mode: u32, truncate: bool) -> io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode & 0o777);
    }
    options.open(path)
}

#[cfg(unix)]
fn create_dir_all_with_mode(path: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(mode & 0o777).create(path)
}

#[cfg(not(unix))]
fn create_dir_all_with_mode(path: &Path, _mode: u32) -> io::Result<()> {
    fs::create_dir_all(path)
}

fn close_file_reader(file: &mut dyn FileReader) -> Result<(), TrzszError> {
    file.close().map_err(|e| TrzszError {
        message: e.to_string(),
        err_type: String::new(),
        trace: false,
    })
}

fn with_closed_file_reader<T>(
    file: &mut dyn FileReader,
    operation: impl FnOnce(&mut dyn FileReader) -> Result<T, TrzszError>,
) -> Result<T, TrzszError> {
    let result = operation(file);
    let close_result = close_file_reader(file);
    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn close_file_writer(file: &mut dyn FileWriter) -> Result<(), TrzszError> {
    file.close().map_err(|e| TrzszError {
        message: e.to_string(),
        err_type: String::new(),
        trace: false,
    })
}

fn with_closed_file_writer<T>(
    file: &mut dyn FileWriter,
    operation: impl FnOnce(&mut dyn FileWriter) -> Result<T, TrzszError>,
) -> Result<T, TrzszError> {
    let result = operation(file);
    let close_result = close_file_writer(file);
    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
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

    #[test]
    fn file_creation_errors_match_go_categories() {
        let path = Path::new("target");
        for (kind, expected) in [
            (
                io::ErrorKind::PermissionDenied,
                "No permission to write: target",
            ),
            (io::ErrorKind::IsADirectory, "Is a directory: target"),
            (io::ErrorKind::NotADirectory, "Not a directory: target"),
        ] {
            let error = file_creation_error(path, io::Error::from(kind));
            assert_eq!(error.message, expected);
        }
    }

    #[test]
    fn recv_check_limited_decodes_go_remote_error_payload() {
        let message = "limited remote error";
        let wire = format!("#FAIL:{}\n", escape::encode_string(message));
        let mut transfer = TrzszTransfer::new(Box::new(std::io::sink()));
        transfer.add_received_data(wire.as_bytes(), false);

        let error = transfer
            .recv_check_limited("ACT", false, None, 128)
            .unwrap_err();
        assert_eq!(error.err_type, "FAIL");
        assert_eq!(error.message, message);
    }

    #[test]
    fn pause_support_is_enabled_only_for_protocol_v3_and_later() {
        let mut transfer = TrzszTransfer::new(Box::new(std::io::sink()));
        let mut action = TransferAction::default();
        for (protocol, expected) in [(2, false), (3, true)] {
            action.protocol = protocol;
            transfer
                .send_config(
                    false,
                    false,
                    false,
                    false,
                    &serde_json::Value::Null,
                    0,
                    &action,
                    CompressType::Auto,
                )
                .unwrap();
            assert_eq!(transfer.pause_supported.load(Ordering::SeqCst), expected);
        }
    }

    #[test]
    fn v4_archives_only_when_not_overwriting() {
        fn source(path_id: i32, name: &str, is_dir: bool) -> SourceFile {
            SourceFile {
                path_id,
                abs_path: std::path::PathBuf::new(),
                rel_path: vec![name.to_string()],
                is_dir,
                archive: false,
                sub_files: Vec::new(),
                size: 0,
                perm: None,
            }
        }

        let files = vec![
            source(0, "bundle", true),
            source(0, "nested.txt", false),
            source(1, "standalone.txt", false),
        ];
        let archived = prepare_source_files(&files, K_PROTOCOL_VERSION4, false);
        assert_eq!(archived.len(), 2);
        assert_eq!(archived[0].sub_files.len(), 1);
        let wire: SourceFile = serde_json::from_str(&archived[0].marshal().unwrap()).unwrap();
        assert!(wire.archive);
        assert!(wire.sub_files.is_empty());

        for (protocol, overwrite) in [(K_PROTOCOL_VERSION4, true), (K_PROTOCOL_VERSION3, false)] {
            let files = prepare_source_files(&files, protocol, overwrite);
            assert_eq!(files.len(), 3);
            assert!(files.iter().all(|file| file.sub_files.is_empty()));
        }
        assert_eq!(K_PROTOCOL_VERSION, K_PROTOCOL_VERSION4);
    }
    #[test]
    fn archive_root_path_cannot_escape_destination() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("download");
        std::fs::create_dir(&destination).unwrap();
        let outside = temp.path().join("escaped");
        let source = SourceFile {
            path_id: 0,
            abs_path: std::path::PathBuf::new(),
            rel_path: vec!["../escaped".to_string()],
            is_dir: true,
            archive: true,
            sub_files: Vec::new(),
            size: 0,
            perm: None,
        };
        let mut transfer = TrzszTransfer::new(Box::new(std::io::sink()));
        let error = transfer
            .create_dir_or_file(&destination, &source, false)
            .err()
            .unwrap();
        assert!(error.message.contains("Invalid archive root path"));
        assert!(!outside.exists());
    }

    struct TrackingWriter(std::sync::Arc<AtomicBool>);

    impl FileWriter for TrackingWriter {
        fn write_all(&mut self, _buf: &[u8]) -> io::Result<()> {
            Ok(())
        }

        fn close(&mut self) -> io::Result<()> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn file_writer_is_closed_after_success_and_failure() {
        for should_fail in [false, true] {
            let closed = std::sync::Arc::new(AtomicBool::new(false));
            let mut file = TrackingWriter(closed.clone());
            let result = with_closed_file_writer(&mut file, |_| {
                if should_fail {
                    Err(crate::comm::simple_error("transfer failed"))
                } else {
                    Ok(())
                }
            });
            assert_eq!(result.is_err(), should_fail);
            assert!(closed.load(Ordering::SeqCst));
        }
    }
    #[test]
    fn tunnel_handshake_routes_protocol_over_tcp() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port() as i32;
        let unique_id = "1234567890100".to_string();
        let uid = unique_id[..unique_id.len() - 2].to_string();
        let mut transfer = TrzszTransfer::new(Box::new(std::io::sink()));
        transfer.accept_on_tunnel(listener, unique_id, port);

        let peer = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let mut invalid = std::net::TcpStream::connect(("127.0.0.1", port as u16)).unwrap();
            invalid.write_all(b"bad hello").unwrap();
            drop(invalid);
            let mut stream = std::net::TcpStream::connect(("127.0.0.1", port as u16)).unwrap();
            let client_hello = format!("::TRZSZ::CLIENT::HELLO::{}:{}", uid, port);
            let server_hello = format!("::TRZSZ::SERVER::HELLO::{}:{}", uid, port);
            stream.write_all(client_hello.as_bytes()).unwrap();
            let mut response = vec![0; server_hello.len()];
            stream.read_exact(&mut response).unwrap();
            assert_eq!(response, server_hello.as_bytes());
            let action = serde_json::json!({
                "lang": "go", "confirm": true, "newline": "\n",
                "tunnel": true, "fork": true
            })
            .to_string();
            let line = format!("#ACT:{}\n", crate::escape::encode_string(&action));
            stream.write_all(line.as_bytes()).unwrap();
            let mut response = vec![0; b"#CFG:ready\n".len()];
            stream.read_exact(&mut response).unwrap();
            assert_eq!(response, b"#CFG:ready\n");
        });

        let action = transfer.recv_action().unwrap();
        assert!(action.tunnel);
        assert!(action.fork);
        assert!(transfer.tunnel_connected);
        transfer.send_line("CFG", "ready").unwrap();
        peer.join().unwrap();
    }
}
