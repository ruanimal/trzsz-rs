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

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod features;
use crate::comm::{self, TrzszError, check_path_writable, check_paths_readable};
use crate::progress::{ProgressCallback, TextProgressBar};
use crate::stop_prompt::StopPromptController;
use crate::transfer::TrzszTransfer;
use crate::version::TrzszVersion;

const TRIGGER_MARKER: &[u8] = b"::TRZSZ:TRANSFER:";
const MAX_TRIGGER_LINE: usize = 512;

/// TrzszOptions configure a client-side [`TrzszFilter`].
#[derive(Debug, Clone, Default)]
pub struct TrzszOptions {
    /// Current terminal width used by the progress bar.
    pub terminal_columns: i32,
    /// Detect shell-quoted POSIX absolute paths pasted into the terminal and upload them.
    pub detect_drag_file: bool,
    /// Detect trace control markers emitted by the remote shell.
    pub detect_trace_log: bool,
    /// Bridge detected ZMODEM sessions to local `sz` / `rz` executables.
    pub enable_zmodem: bool,
    /// Decode OSC52 output and send its clipboard text to a configured host callback.
    pub enable_osc52: bool,
}

/// Parsed transfer request announced by the remote endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrzszTrigger {
    pub mode: char,
    pub version: Option<TrzszVersion>,
    pub unique_id: String,
    pub win_server: bool,
    pub tunnel_port: i32,
    pub tmux_prefix: String,
    pub tmux_pane_id: String,
}

/// Progress notification suitable for embedding applications.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressEvent {
    pub file_count: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_step: i64,
    pub done: bool,
    pub pausing: bool,
}

type UploadSelector = Arc<
    dyn Fn(bool, Option<PathBuf>) -> Result<Option<Vec<PathBuf>>, TrzszError>
        + Send
        + Sync
        + 'static,
>;
type DownloadSelector =
    Arc<dyn Fn(Option<PathBuf>) -> Result<Option<PathBuf>, TrzszError> + Send + Sync + 'static>;
type StateCallback = Arc<dyn Fn(bool) + Send + Sync + 'static>;
type RedrawCallback = Arc<dyn Fn() + Send + Sync + 'static>;
type ProgressObserver = Arc<dyn Fn(ProgressEvent) + Send + Sync + 'static>;
type ShutdownCallback = Arc<dyn Fn() + Send + Sync + 'static>;

/// Client-side trzsz protocol filter.
///
/// `new` only stores the four I/O endpoints. Call [`run`](Self::run) on a
/// dedicated thread to start the bidirectional pumps. `close` may be called
/// concurrently to stop the run loop. Generic blocking `Read` implementations
/// cannot be forcibly interrupted by Rust; for such endpoints, install shutdown
/// callbacks with [`set_shutdown_handlers`](Self::set_shutdown_handlers) so
/// `close` can wake their blocked reads.
pub struct TrzszFilter {
    client_in: Mutex<Option<Box<dyn Read + Send>>>,
    client_out: SharedEndpoint,
    server_in: SharedEndpoint,
    server_out: Mutex<Option<Box<dyn Read + Send>>>,
    pub options: TrzszOptions,
    closed: AtomicBool,
    running: AtomicBool,
    transferring: Arc<AtomicBool>,
    active_controller: Arc<Mutex<Option<StopPromptController>>>,
    columns: Arc<std::sync::atomic::AtomicI32>,
    current_trigger: Mutex<Option<TrzszTrigger>>,
    default_upload_path: Mutex<Option<PathBuf>>,
    default_download_path: Mutex<Option<PathBuf>>,
    upload_selector: Mutex<Option<UploadSelector>>,
    download_selector: Mutex<Option<DownloadSelector>>,
    pending_upload: Mutex<Option<PendingUpload>>,
    state_callback: Mutex<Option<StateCallback>>,
    redraw_callback: Mutex<Option<RedrawCallback>>,
    progress_observer: Mutex<Option<ProgressObserver>>,
    clipboard_callback: Mutex<Option<Arc<dyn Fn(String) + Send + Sync + 'static>>>,
    zmodem_commands: Mutex<(std::ffi::OsString, std::ffi::OsString)>,
    client_input_shutdown: Mutex<Option<ShutdownCallback>>,
    server_output_shutdown: Mutex<Option<ShutdownCallback>>,
}

struct PendingUpload {
    paths: Vec<PathBuf>,
    result: Option<mpsc::Sender<Result<(), String>>>,
}

#[derive(Clone)]
struct SharedEndpoint(Arc<Mutex<Option<Box<dyn Write + Send>>>>);

impl SharedEndpoint {
    fn new(writer: Box<dyn Write + Send>) -> Self {
        SharedEndpoint(Arc::new(Mutex::new(Some(writer))))
    }

    fn close(&self) {
        self.0.lock().unwrap().take();
    }

    fn writer(&self) -> SharedEndpointWriter {
        SharedEndpointWriter(self.clone())
    }
}

struct SharedEndpointWriter(SharedEndpoint);

impl Write for SharedEndpointWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut endpoint = self.0.0.lock().unwrap();
        let writer = endpoint
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "filter endpoint closed"))?;
        writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut endpoint = self.0.0.lock().unwrap();
        let writer = endpoint
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "filter endpoint closed"))?;
        writer.flush()
    }
}

enum PumpEvent {
    ClientData(Vec<u8>),
    ClientEof,
    ClientError(io::Error),
    ServerData(Vec<u8>),
    ServerEof,
    ServerError(io::Error),
    TransferDone,
    ZmodemData(Vec<u8>),
    ZmodemDone,
}

struct TriggerDetector {
    pending: Vec<u8>,
}

impl TriggerDetector {
    fn new() -> Self {
        TriggerDetector {
            pending: Vec::new(),
        }
    }

    fn push(&mut self, bytes: &[u8]) -> (Vec<u8>, Option<TrzszTrigger>, Vec<u8>) {
        self.pending.extend_from_slice(bytes);
        let mut output = Vec::new();
        loop {
            let Some(idx) = find_subslice(&self.pending, TRIGGER_MARKER) else {
                let keep = longest_marker_prefix_suffix(&self.pending);
                let emit = self.pending.len().saturating_sub(keep);
                output.extend(self.pending.drain(..emit));
                return (output, None, Vec::new());
            };
            output.extend(self.pending.drain(..idx));
            let Some(end) = self.pending.iter().position(|&b| b == b'\r' || b == b'\n') else {
                if self.pending.len() > MAX_TRIGGER_LINE {
                    output.push(self.pending.remove(0));
                    continue;
                }
                return (output, None, Vec::new());
            };
            let mut line_end = end + 1;
            if self.pending[end] == b'\r' && self.pending.get(end + 1) == Some(&b'\n') {
                line_end += 1;
            }
            let line = self.pending[..line_end].to_vec();
            self.pending.drain(..line_end);
            if let Some(trigger) = parse_trigger(&line) {
                output.extend_from_slice(&rewrite_trigger(&line));
                let remainder = std::mem::take(&mut self.pending);
                return (output, Some(trigger), remainder);
            }
            output.extend_from_slice(&line);
        }
    }

    fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.pending)
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
}

fn longest_marker_prefix_suffix(bytes: &[u8]) -> usize {
    (1..TRIGGER_MARKER.len().min(bytes.len() + 1))
        .rev()
        .find(|&count| bytes.ends_with(&TRIGGER_MARKER[..count]))
        .unwrap_or(0)
}

fn parse_trigger(line: &[u8]) -> Option<TrzszTrigger> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let marker_idx = find_subslice(line, TRIGGER_MARKER)?;
    let body = std::str::from_utf8(&line[marker_idx + TRIGGER_MARKER.len()..]).ok()?;
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() < 2 || parts.len() > 4 || !matches!(parts[0], "S" | "R" | "D") {
        return None;
    }
    let version = TrzszVersion::parse(parts[1])?;
    let unique_id = parts.get(2).copied().unwrap_or_default();
    if !unique_id.is_empty() && !unique_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let tunnel_port = match parts.get(3).copied().filter(|value| !value.is_empty()) {
        Some(value) => {
            let value = value.strip_suffix("#R").unwrap_or(value);
            if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            value.parse::<i32>().ok()?
        }
        None => 0,
    };
    let unique_id = unique_id.to_string();
    let win_server = unique_id == "1" || (unique_id.len() == 13 && unique_id.ends_with("10"));
    Some(TrzszTrigger {
        mode: parts[0].chars().next()?,
        version: Some(version),
        unique_id,
        win_server,
        tunnel_port,
        tmux_prefix: String::new(),
        tmux_pane_id: String::new(),
    })
}

fn rewrite_trigger(line: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(line.len() + 3);
    let mut index = 0;
    while index < line.len() {
        if line[index..].starts_with(b"TRZSZ:") {
            output.extend_from_slice(b"TRZSZGO:");
            index += b"TRZSZ:".len();
        } else {
            output.push(line[index]);
            index += 1;
        }
    }
    output
}

fn expand_home_path(path: &Path) -> PathBuf {
    let value = path.to_string_lossy();
    if value == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| path.to_path_buf());
    }
    if let Some(remainder) = value
        .strip_prefix("~/")
        .or_else(|| value.strip_prefix("~\\"))
    {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(remainder);
        }
    }
    path.to_path_buf()
}

impl TrzszFilter {
    /// Construct a filter around the client/server I/O endpoints.
    ///
    /// No threads are started until [`run`](Self::run) is called.
    pub fn new(
        client_in: Box<dyn Read + Send>,
        client_out: Box<dyn Write + Send>,
        server_in: Box<dyn Write + Send>,
        server_out: Box<dyn Read + Send>,
        options: TrzszOptions,
    ) -> Self {
        let columns = options.terminal_columns;
        TrzszFilter {
            client_in: Mutex::new(Some(client_in)),
            client_out: SharedEndpoint::new(client_out),
            server_in: SharedEndpoint::new(server_in),
            server_out: Mutex::new(Some(server_out)),
            options,
            closed: AtomicBool::new(false),
            running: AtomicBool::new(false),
            transferring: Arc::new(AtomicBool::new(false)),
            active_controller: Arc::new(Mutex::new(None)),
            columns: Arc::new(std::sync::atomic::AtomicI32::new(columns)),
            current_trigger: Mutex::new(None),
            default_upload_path: Mutex::new(None),
            default_download_path: Mutex::new(None),
            upload_selector: Mutex::new(None),
            download_selector: Mutex::new(None),
            pending_upload: Mutex::new(None),
            state_callback: Mutex::new(None),
            clipboard_callback: Mutex::new(None),
            zmodem_commands: Mutex::new(("sz".into(), "rz".into())),
            redraw_callback: Mutex::new(None),
            progress_observer: Mutex::new(None),
            client_input_shutdown: Mutex::new(None),
            server_output_shutdown: Mutex::new(None),
        }
    }

    /// Run both I/O pumps and handle transfer triggers until an endpoint closes.
    ///
    /// This is a blocking, single-use operation; call it on a dedicated thread
    /// when the host needs to interact with the filter concurrently.
    pub fn run(&self) -> io::Result<()> {
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "filter run may only be called once",
            ));
        }
        if self.closed.load(Ordering::SeqCst) {
            self.finish_runtime();
            return Ok(());
        }
        let client_in = self.client_in.lock().unwrap().take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "client input already consumed")
        })?;
        let server_out = self.server_out.lock().unwrap().take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "server output already consumed",
            )
        })?;

        let (sender, receiver) = mpsc::sync_channel(64);
        if let Err(error) = spawn_reader(client_in, sender.clone(), true) {
            self.finish_runtime();
            return Err(error);
        }
        if let Err(error) = spawn_reader(server_out, sender.clone(), false) {
            self.finish_runtime();
            return Err(error);
        }
        let result = self.run_events(receiver, sender.clone());
        self.finish_runtime();
        result
    }

    /// Stop the filter and wake any active transfer.
    ///
    /// For blocking host-provided readers, install shutdown callbacks so their
    /// reads are woken and their reader threads can exit promptly.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(controller) = self.active_controller.lock().unwrap().as_ref() {
            controller.stop(false);
        }
        self.server_in.close();
        self.call_shutdown_handlers();
    }

    /// Set callbacks which interrupt client-input/server-output readers on close.
    pub fn set_shutdown_handlers(
        &self,
        client_input: Option<Arc<dyn Fn() + Send + Sync>>,
        server_output: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        *self.client_input_shutdown.lock().unwrap() = client_input;
        *self.server_output_shutdown.lock().unwrap() = server_output;
    }

    /// Return whether a transfer is currently in progress.
    pub fn is_transferring_files(&self) -> bool {
        self.transferring.load(Ordering::SeqCst)
    }

    /// Stop the current transfer, optionally deleting partially received files.
    pub fn stop_transferring_files(&self, stop_and_delete: bool) {
        if let Some(controller) = self.active_controller.lock().unwrap().as_ref() {
            controller.stop(stop_and_delete);
        }
    }

    /// Update the terminal width used by progress rendering.
    pub fn set_terminal_columns(&self, columns: i32) {
        self.options_terminal_columns(columns);
    }

    fn options_terminal_columns(&self, columns: i32) {
        self.columns.store(columns, Ordering::Relaxed);
    }

    /// Set the suggested starting path for the host's upload picker.
    pub fn set_default_upload_path(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();
        *self.default_upload_path.lock().unwrap() =
            (!path.as_os_str().is_empty()).then(|| expand_home_path(path));
    }

    /// Set the default client-side download directory used when no selector is set.
    pub fn set_default_download_path(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();
        *self.default_download_path.lock().unwrap() =
            (!path.as_os_str().is_empty()).then(|| expand_home_path(path));
    }

    /// Register the host's upload picker. The first argument says whether
    /// directories are permitted and the second is its suggested start path.
    /// Returning `Ok(None)` means the user cancelled.
    pub fn set_upload_path_selector<F>(&self, selector: F)
    where
        F: Fn(bool, Option<PathBuf>) -> Result<Option<Vec<PathBuf>>, TrzszError>
            + Send
            + Sync
            + 'static,
    {
        *self.upload_selector.lock().unwrap() = Some(Arc::new(selector));
    }

    /// Register the host's download-directory picker and suggested start path.
    pub fn set_download_path_selector<F>(&self, selector: F)
    where
        F: Fn(Option<PathBuf>) -> Result<Option<PathBuf>, TrzszError> + Send + Sync + 'static,
    {
        *self.download_selector.lock().unwrap() = Some(Arc::new(selector));
    }

    /// Notify the host when a transfer starts (`true`) or finishes (`false`).
    pub fn set_transfer_state_callback<F>(&self, callback: F)
    where
        F: Fn(bool) + Send + Sync + 'static,
    {
        *self.state_callback.lock().unwrap() = Some(Arc::new(callback));
    }

    pub fn clear_transfer_state_callback(&self) {
        *self.state_callback.lock().unwrap() = None;
    }

    /// Register a callback to redraw the host UI after a transfer finishes.
    pub fn set_redraw_screen_func<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.redraw_callback.lock().unwrap() = Some(Arc::new(callback));
    }

    pub fn clear_redraw_screen_func(&self) {
        *self.redraw_callback.lock().unwrap() = None;
    }

    /// Register an observer for file count, name, byte steps, and completion.
    pub fn set_progress_callback<F>(&self, callback: F)
    where
        F: Fn(ProgressEvent) + Send + Sync + 'static,
    {
        *self.progress_observer.lock().unwrap() = Some(Arc::new(callback));
    }

    pub fn clear_progress_callback(&self) {
        *self.progress_observer.lock().unwrap() = None;
    }

    /// Configure the host clipboard sink used for decoded OSC52 text.
    /// No platform clipboard is accessed by the filter itself.
    pub fn set_clipboard_callback<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        *self.clipboard_callback.lock().unwrap() = Some(Arc::new(callback));
    }

    pub fn clear_clipboard_callback(&self) {
        *self.clipboard_callback.lock().unwrap() = None;
    }

    /// Override the local ZMODEM command paths (defaults to `sz` and `rz`).
    pub fn set_zmodem_commands(&self, sz: impl AsRef<Path>, rz: impl AsRef<Path>) {
        *self.zmodem_commands.lock().unwrap() = (
            sz.as_ref().as_os_str().to_os_string(),
            rz.as_ref().as_os_str().to_os_string(),
        );
    }

    /// Read upload/download defaults from `$HOME/.trzsz.conf` without replacing
    /// values already configured through the API.
    pub fn read_trzsz_config(&self) {
        let Ok(home) = std::env::var("HOME") else {
            return;
        };
        let Ok(content) = std::fs::read_to_string(Path::new(&home).join(".trzsz.conf")) else {
            return;
        };
        for line in content.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim();
            if value.is_empty() {
                continue;
            }
            match name.as_str() {
                "defaultuploadpath" => {
                    let mut path = self.default_upload_path.lock().unwrap();
                    if path.is_none() {
                        *path = Some(expand_home_path(Path::new(value)));
                    }
                }
                "defaultdownloadpath" => {
                    let mut path = self.default_download_path.lock().unwrap();
                    if path.is_none() {
                        *path = Some(expand_home_path(Path::new(value)));
                    }
                }
                _ => {}
            }
        }
    }

    /// Queue an upload and interrupt the remote shell to invoke `trz`.
    /// Returns once the request has been queued; completion is reflected by the
    /// transfer-state callback. Use [`one_time_upload`](Self::one_time_upload)
    /// when a remote `trz` process is already waiting for the transfer.
    pub fn upload_files(&self, paths: &[PathBuf]) -> Result<(), TrzszError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(comm::simple_error("Filter is not running"));
        }
        let request = make_upload_request(paths, None)?;
        self.queue_upload(request)?;
        let command = if paths.iter().any(|path| path.is_dir()) {
            b"trz -d\r".as_slice()
        } else {
            b"trz\r".as_slice()
        };
        let mut writer = self.server_in.writer();
        let interrupt_result = writer.write_all(b"\x03").and_then(|_| writer.flush());
        if let Err(error) = interrupt_result {
            *self.pending_upload.lock().unwrap() = None;
            return Err(comm::simple_trzsz_error(
                "Interrupt remote shell failed",
                error,
            ));
        }
        thread::sleep(Duration::from_millis(200));
        if let Err(error) = writer.write_all(command).and_then(|_| writer.flush()) {
            *self.pending_upload.lock().unwrap() = None;
            return Err(comm::simple_trzsz_error("Start remote trz failed", error));
        }
        Ok(())
    }

    /// Queue files for the next remote `trz`/`trz -d` trigger without issuing a
    /// remote command. The returned receiver resolves after that transfer ends.
    pub fn one_time_upload(
        &self,
        paths: &[PathBuf],
    ) -> Result<Receiver<Result<(), String>>, TrzszError> {
        let (sender, receiver) = mpsc::channel();
        let request = make_upload_request(paths, Some(sender))?;
        self.queue_upload(request)?;
        Ok(receiver)
    }

    fn queue_upload(&self, request: PendingUpload) -> Result<(), TrzszError> {
        let mut pending = self.pending_upload.lock().unwrap();
        if pending.is_some() {
            return Err(comm::simple_error("An upload is already queued"));
        }
        *pending = Some(request);
        Ok(())
    }

    /// Detect a complete transfer trigger in a byte slice.
    ///
    /// The streaming run loop additionally handles markers split across reads.
    pub fn detect_trzsz(buf: &[u8]) -> (Vec<u8>, Option<TrzszTrigger>) {
        let Some(idx) = find_subslice(buf, TRIGGER_MARKER) else {
            return (buf.to_vec(), None);
        };
        let tail = &buf[idx..];
        let Some(end) = tail.iter().position(|&b| b == b'\r' || b == b'\n') else {
            return (buf.to_vec(), None);
        };
        let end = end + 1;
        let mut line_end = end;
        if tail[end - 1] == b'\r' && tail.get(end) == Some(&b'\n') {
            line_end += 1;
        }
        let Some(trigger) = parse_trigger(&tail[..line_end]) else {
            return (buf.to_vec(), None);
        };
        let mut output = buf[..idx].to_vec();
        output.extend_from_slice(&rewrite_trigger(&tail[..line_end]));
        output.extend_from_slice(&tail[line_end..]);
        (output, Some(trigger))
    }

    fn run_events(
        &self,
        receiver: Receiver<PumpEvent>,
        sender: SyncSender<PumpEvent>,
    ) -> io::Result<()> {
        let mut detector = TriggerDetector::new();
        let mut osc52 = features::osc52::Osc52Parser::default();
        let mut trace = features::trace::TraceLogger::default();
        let mut drag = features::drag::DragInput::default();
        let mut zmodem_detector = features::zmodem::ZmodemDetector::default();
        let mut zmodem_server_finish_detector = features::zmodem::ZmodemFinishDetector::default();
        let mut zmodem_client_finish_detector = features::zmodem::ZmodemFinishDetector::default();
        let mut zmodem: Option<features::zmodem::ZmodemProcess> = None;
        let mut zmodem_upload = false;
        let mut zmodem_server_finished = false;
        let mut zmodem_client_finished = false;
        let mut zmodem_last_activity = None;
        let mut zmodem_timed_out = false;
        let mut zmodem_over_and_out_sent = false;
        let mut tmux = features::tmuxcc::TmuxControlDecoder::default();
        let mut tmux_input = features::tmuxcc::TmuxInputDecoder::default();
        let mut client_eof = false;
        let mut server_eof = false;
        let mut transfer_input: Option<SyncSender<Vec<u8>>> = None;
        loop {
            if self.closed.load(Ordering::SeqCst) {
                if let Some(process) = zmodem.as_mut() {
                    let _ = process.child.kill();
                    let _ = process.child.wait();
                }
                return Ok(());
            }
            let event = match receiver.recv_timeout(Duration::from_millis(50)) {
                Ok(event) => event,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !zmodem_timed_out
                        && zmodem_last_activity.is_some_and(|last| {
                            features::zmodem::idle_timeout_elapsed(last, Instant::now())
                        })
                    {
                        zmodem_timed_out = true;
                        if let Some(process) = zmodem.as_mut() {
                            process.stdin.take();
                            let _ = process.child.kill();
                        }
                        let mut server = self.server_in.writer();
                        let _ = server
                            .write_all(features::zmodem::CANCEL_SEQUENCE)
                            .and_then(|_| server.flush());
                        let mut client = self.client_out.writer();
                        let _ = client
                            .write_all(b"\r\nZMODEM transfer timed out\r\n")
                            .and_then(|_| client.flush());
                    }
                    if self.options.detect_drag_file
                        && drag.is_pending()
                        && drag.should_finish(Instant::now())
                    {
                        let (held, paths) = drag.finish();
                        if paths
                            .as_ref()
                            .is_some_and(|paths| self.upload_files(paths).is_ok())
                        {
                            continue;
                        }
                        let mut writer = self.server_in.writer();
                        writer.write_all(&held)?;
                        writer.flush()?;
                    }
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            };
            match event {
                PumpEvent::ClientData(bytes) => {
                    if self.options.detect_trace_log {
                        trace.log("stdin", &bytes);
                    }
                    if let Some(process) = zmodem.as_mut() {
                        zmodem_last_activity = Some(Instant::now());
                        if bytes.as_slice() == [0x03] {
                            let mut writer = self.server_in.writer();
                            let _ = writer
                                .write_all(features::zmodem::CANCEL_SEQUENCE)
                                .and_then(|_| writer.flush());
                            process.stdin.take();
                            let _ = process.child.kill();
                            zmodem_timed_out = true;
                        } else if let Some(stdin) = process.stdin.as_mut() {
                            if stdin.write_all(&bytes).and_then(|_| stdin.flush()).is_err() {
                                let _ = process.child.kill();
                            }
                        }
                        continue;
                    }
                    if let Some(trigger) = self
                        .current_trigger
                        .lock()
                        .unwrap()
                        .clone()
                        .filter(|trigger| !trigger.tmux_pane_id.is_empty())
                    {
                        for action in tmux_input.feed(&bytes, &trigger.tmux_pane_id) {
                            match action {
                                features::tmuxcc::TmuxClientInput::CurrentPane(keys) => {
                                    if let Some(controller) =
                                        self.active_controller.lock().unwrap().clone()
                                    {
                                        let mut output = self.client_out.writer();
                                        for key in keys {
                                            controller.handle_filter_input(key, &mut output);
                                        }
                                    }
                                    let mut output = self.client_out.writer();
                                    output.write_all(&features::tmuxcc::client_input_ack())?;
                                    output.flush()?;
                                }
                                features::tmuxcc::TmuxClientInput::Forward(command) => {
                                    let _ = tmux.ack.issue(false);
                                    let mut framed = command;
                                    framed.push(b'\r');
                                    let mut writer = self.server_in.writer();
                                    writer.write_all(&framed)?;
                                    writer.flush()?;
                                }
                            }
                        }
                        continue;
                    }
                    let controller = self.active_controller.lock().unwrap().clone();
                    if let Some(controller) = controller {
                        let mut output = self.client_out.writer();
                        for key in bytes {
                            controller.handle_filter_input(key, &mut output);
                        }
                        continue;
                    }
                    if self.options.detect_drag_file {
                        let now = Instant::now();
                        if drag.is_pending() || drag.push(&bytes, now) {
                            if drag.should_finish(now) {
                                let (held, paths) = drag.finish();
                                if paths
                                    .as_ref()
                                    .is_some_and(|paths| self.upload_files(paths).is_ok())
                                {
                                    continue;
                                }
                                let mut writer = self.server_in.writer();
                                writer.write_all(&held)?;
                                writer.flush()?;
                            }
                            continue;
                        }
                    }
                    let mut writer = self.server_in.writer();
                    writer.write_all(&bytes)?;
                    writer.flush()?;
                }
                PumpEvent::ClientEof => {
                    client_eof = true;
                    if let Some((held, paths)) = drag.is_pending().then(|| drag.finish()) {
                        if paths
                            .as_ref()
                            .is_some_and(|paths| self.upload_files(paths).is_ok())
                        {
                            // The upload command is queued before the endpoint is closed.
                        } else {
                            let mut writer = self.server_in.writer();
                            writer.write_all(&held)?;
                            writer.flush()?;
                        }
                    }
                    if let Some(process) = zmodem.as_mut() {
                        let _ = process.child.kill();
                    }
                    if let Some(controller) = self.active_controller.lock().unwrap().as_ref() {
                        controller.handle_filter_eof();
                        controller.stop(false);
                    }
                    if let Some(features::tmuxcc::TmuxClientInput::Forward(pending)) =
                        tmux_input.finish()
                    {
                        let mut writer = self.server_in.writer();
                        writer.write_all(&pending)?;
                        writer.flush()?;
                    }
                    self.server_in.close();
                    let shutdown = self.server_output_shutdown.lock().unwrap().clone();
                    if let Some(shutdown) = shutdown {
                        shutdown();
                    }
                    if server_eof && transfer_input.is_none() && zmodem.is_none() {
                        return Ok(());
                    }
                }
                PumpEvent::ClientError(_error) if server_eof => {
                    client_eof = true;
                    if let Some(controller) = self.active_controller.lock().unwrap().as_ref() {
                        controller.stop(false);
                    }
                    if let Some(process) = zmodem.as_mut() {
                        let _ = process.child.kill();
                    }
                    if transfer_input.is_none() && zmodem.is_none() {
                        return Ok(());
                    }
                }
                PumpEvent::ClientError(error) => return Err(error),
                PumpEvent::ServerData(mut bytes) => {
                    let tmux_input = tmux.should_parse(&bytes);
                    let transfer_active = self.transferring.load(Ordering::SeqCst);
                    if !tmux_input {
                        if let Some(input) = transfer_input.as_ref() {
                            if transfer_active {
                                match input.send(bytes) {
                                    Ok(()) => continue,
                                    Err(error) => {
                                        transfer_input = None;
                                        bytes = error.0;
                                    }
                                }
                            } else {
                                transfer_input = None;
                            }
                        }
                    }
                    let tmux_transfer = tmux_input
                        && transfer_active
                        && self
                            .current_trigger
                            .lock()
                            .unwrap()
                            .as_ref()
                            .is_some_and(|trigger| !trigger.tmux_pane_id.is_empty());
                    if !tmux_transfer && self.options.detect_trace_log {
                        trace.log("svrout", &bytes);
                        if zmodem.is_none() {
                            bytes = trace.process_server_output(&bytes);
                        }
                    }
                    if !tmux_transfer && zmodem.is_none() && self.options.enable_osc52 {
                        let values = osc52.push(&bytes);
                        if let Some(callback) = self.clipboard_callback.lock().unwrap().clone() {
                            for value in values {
                                callback(String::from_utf8_lossy(&value).into_owned());
                            }
                        }
                    }
                    if let Some(process) = zmodem.as_mut() {
                        zmodem_last_activity = Some(Instant::now());
                        let server_finished = zmodem_server_finish_detector.push(&bytes);
                        let server_cancelled = bytes
                            .windows(5)
                            .any(|window| window == b"\x18\x18\x18\x18\x18");
                        let write_result = process.stdin.as_mut().map_or_else(
                            || Err(io::Error::other("ZMODEM process stdin closed")),
                            |stdin| stdin.write_all(&bytes).and_then(|_| stdin.flush()),
                        );
                        if server_finished {
                            zmodem_server_finished = true;
                        }
                        if write_result.is_err() || server_cancelled {
                            process.stdin.take();
                            let _ = process.child.kill();
                        } else if zmodem_server_finished
                            && zmodem_client_finished
                            && !zmodem_over_and_out_sent
                        {
                            if zmodem_upload {
                                let mut writer = self.server_in.writer();
                                writer.write_all(features::zmodem::OVER_AND_OUT)?;
                                writer.flush()?;
                            } else {
                                if let Some(stdin) = process.stdin.as_mut() {
                                    let _ = stdin.write_all(features::zmodem::OVER_AND_OUT);
                                }
                                process.stdin.take();
                            }
                            zmodem_over_and_out_sent = true;
                        }
                        continue;
                    }
                    if tmux_input {
                        for record in tmux.feed(&bytes) {
                            match record {
                                features::tmuxcc::TmuxRecord::Visible(output) => {
                                    let mut writer = self.client_out.writer();
                                    writer.write_all(&output)?;
                                    writer.flush()?;
                                }
                                features::tmuxcc::TmuxRecord::Output {
                                    pane_id,
                                    prefix,
                                    bytes: pane_bytes,
                                    raw_line,
                                } => {
                                    if self.options.enable_osc52 && !tmux_transfer {
                                        let values = osc52.push(&pane_bytes);
                                        if let Some(callback) =
                                            self.clipboard_callback.lock().unwrap().clone()
                                        {
                                            for value in values {
                                                callback(
                                                    String::from_utf8_lossy(&value).into_owned(),
                                                );
                                            }
                                        }
                                    }
                                    if let (Some(input), Some(trigger)) = (
                                        transfer_input.as_ref(),
                                        self.current_trigger.lock().unwrap().clone(),
                                    ) {
                                        if trigger.tmux_pane_id == pane_id
                                            && self.transferring.load(Ordering::SeqCst)
                                        {
                                            let _ = input.send(pane_bytes);
                                            continue;
                                        }
                                        let mut writer = self.client_out.writer();
                                        writer.write_all(&raw_line)?;
                                        writer.flush()?;
                                        continue;
                                    }
                                    let (output, trigger, trailing) = detector.push(&pane_bytes);
                                    if let Some(mut trigger) = trigger {
                                        trigger.tmux_pane_id = pane_id;
                                        trigger.tmux_prefix = prefix.clone();
                                        *self.current_trigger.lock().unwrap() =
                                            Some(trigger.clone());
                                        transfer_input = Some(self.start_transfer(
                                            trigger,
                                            sender.clone(),
                                            tmux.ack.clone(),
                                        )?);
                                        if !output.is_empty() {
                                            let mut writer = self.client_out.writer();
                                            writer.write_all(
                                                &features::tmuxcc::encode_tmux_output(
                                                    &prefix, &output,
                                                ),
                                            )?;
                                            writer.flush()?;
                                        }
                                        if !trailing.is_empty() {
                                            if let Some(input) = transfer_input.as_ref() {
                                                let _ = input.send(trailing);
                                            }
                                        }
                                    } else if !output.is_empty() {
                                        let mut writer = self.client_out.writer();
                                        writer.write_all(&features::tmuxcc::encode_tmux_output(
                                            &prefix, &output,
                                        ))?;
                                        writer.flush()?;
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    if self.options.enable_zmodem {
                        let (ordinary, init) = zmodem_detector.push(&bytes);
                        if let Some((init, protocol)) = init {
                            if !ordinary.is_empty() {
                                let mut writer = self.client_out.writer();
                                writer.write_all(&ordinary)?;
                                writer.flush()?;
                            }
                            let upload = init.upload;
                            match self.spawn_zmodem_client(init) {
                                Ok(mut process) => {
                                    process
                                        .stdin
                                        .as_mut()
                                        .ok_or_else(|| {
                                            io::Error::other("ZMODEM process stdin unavailable")
                                        })?
                                        .write_all(&protocol)?;
                                    let stdout = process.take_stdout()?;
                                    let events = sender.clone();
                                    thread::Builder::new()
                                        .name("trzsz-filter-zmodem-output".to_string())
                                        .spawn(move || pump_zmodem_stdout(stdout, events))?;
                                    zmodem = Some(process);
                                    zmodem_upload = upload;
                                    zmodem_server_finished = false;
                                    zmodem_client_finished = false;
                                    zmodem_server_finish_detector =
                                        features::zmodem::ZmodemFinishDetector::default();
                                    zmodem_client_finish_detector =
                                        features::zmodem::ZmodemFinishDetector::default();
                                    zmodem_last_activity = Some(Instant::now());
                                    zmodem_timed_out = false;
                                    zmodem_over_and_out_sent = false;
                                    if let Some(callback) =
                                        self.state_callback.lock().unwrap().clone()
                                    {
                                        callback(true);
                                    }
                                }
                                Err(error) => {
                                    let mut writer = self.client_out.writer();
                                    writer.write_all(
                                        format!("\r\nZMODEM start failed: {error}\r\n").as_bytes(),
                                    )?;
                                    writer.write_all(&protocol)?;
                                    writer.flush()?;
                                }
                            }
                            continue;
                        }
                        bytes = ordinary;
                    }
                    let (output, trigger, trailing) = detector.push(&bytes);
                    if !output.is_empty() {
                        let mut writer = self.client_out.writer();
                        writer.write_all(&output)?;
                        writer.flush()?;
                    }
                    if let Some(trigger) = trigger {
                        *self.current_trigger.lock().unwrap() = Some(trigger.clone());
                        transfer_input =
                            Some(self.start_transfer(trigger, sender.clone(), tmux.ack.clone())?);
                        if !trailing.is_empty() {
                            if let Some(input) = transfer_input.as_ref() {
                                let _ = input.send(trailing);
                            }
                        }
                    }
                }
                PumpEvent::ZmodemData(bytes) => {
                    zmodem_last_activity = Some(Instant::now());
                    if zmodem_client_finish_detector.push(&bytes) {
                        zmodem_client_finished = true;
                    }
                    let mut writer = self.server_in.writer();
                    writer.write_all(&bytes)?;
                    writer.flush()?;
                    if zmodem_client_finished && zmodem_server_finished && !zmodem_over_and_out_sent
                    {
                        if zmodem_upload {
                            writer.write_all(features::zmodem::OVER_AND_OUT)?;
                            writer.flush()?;
                        } else {
                            if let Some(process) = zmodem.as_mut() {
                                if let Some(stdin) = process.stdin.as_mut() {
                                    let _ = stdin.write_all(features::zmodem::OVER_AND_OUT);
                                }
                                process.stdin.take();
                            }
                        }
                        zmodem_over_and_out_sent = true;
                    }
                }
                PumpEvent::ZmodemDone => {
                    if let Some(mut process) = zmodem.take() {
                        process.stdin.take();
                        let _ = features::zmodem::wait_child(&mut process.child);
                        if let Some(callback) = self.state_callback.lock().unwrap().clone() {
                            callback(false);
                        }
                    }
                    zmodem_upload = false;
                    zmodem_server_finished = false;
                    zmodem_client_finished = false;
                    zmodem_server_finish_detector =
                        features::zmodem::ZmodemFinishDetector::default();
                    zmodem_client_finish_detector =
                        features::zmodem::ZmodemFinishDetector::default();
                    zmodem_last_activity = None;
                    zmodem_timed_out = false;
                    zmodem_over_and_out_sent = false;
                }
                PumpEvent::ServerEof => {
                    server_eof = true;
                    if let Some(process) = zmodem.as_mut() {
                        let _ = process.child.kill();
                    }
                    if let Some(controller) = self.active_controller.lock().unwrap().as_ref() {
                        controller.stop(false);
                    }
                    if self.options.enable_osc52 {
                        osc52.finish();
                    }
                    if self.options.enable_zmodem {
                        let pending = zmodem_detector.finish();
                        if !pending.is_empty() {
                            let mut writer = self.client_out.writer();
                            writer.write_all(&pending)?;
                            writer.flush()?;
                        }
                    }
                    for record in tmux.finish() {
                        match record {
                            features::tmuxcc::TmuxRecord::Visible(output) => {
                                let mut writer = self.client_out.writer();
                                writer.write_all(&output)?;
                                writer.flush()?;
                            }
                            features::tmuxcc::TmuxRecord::Output {
                                pane_id,
                                bytes,
                                raw_line,
                                ..
                            } => {
                                let trigger = self.current_trigger.lock().unwrap().clone();
                                if trigger
                                    .as_ref()
                                    .is_some_and(|trigger| trigger.tmux_pane_id == pane_id)
                                    && self.transferring.load(Ordering::SeqCst)
                                {
                                    if let Some(input) = transfer_input.as_ref() {
                                        let _ = input.send(bytes);
                                        continue;
                                    }
                                }
                                let mut writer = self.client_out.writer();
                                writer.write_all(&raw_line)?;
                                writer.flush()?;
                            }
                        }
                    }
                    let remaining = detector.finish();
                    if !remaining.is_empty() {
                        let mut writer = self.client_out.writer();
                        writer.write_all(&remaining)?;
                        writer.flush()?;
                    }
                    let remaining = trace.finish();
                    if !remaining.is_empty() {
                        let mut writer = self.client_out.writer();
                        writer.write_all(&remaining)?;
                        writer.flush()?;
                    }
                    let shutdown = self.client_input_shutdown.lock().unwrap().clone();
                    if let Some(shutdown) = shutdown {
                        shutdown();
                    }
                    if client_eof && transfer_input.is_none() && zmodem.is_none() {
                        return Ok(());
                    }
                }
                PumpEvent::ServerError(_error) if client_eof => {
                    server_eof = true;
                    if let Some(process) = zmodem.as_mut() {
                        let _ = process.child.kill();
                    }
                    if let Some(controller) = self.active_controller.lock().unwrap().as_ref() {
                        controller.stop(false);
                    }
                    if transfer_input.is_none() && zmodem.is_none() {
                        return Ok(());
                    }
                }
                PumpEvent::ServerError(error) => return Err(error),
                PumpEvent::TransferDone => {
                    transfer_input = None;
                    self.transferring.store(false, Ordering::SeqCst);
                    *self.active_controller.lock().unwrap() = None;
                    *self.current_trigger.lock().unwrap() = None;
                    if client_eof && server_eof {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn spawn_zmodem_client(
        &self,
        init: features::zmodem::ZmodemInit,
    ) -> io::Result<features::zmodem::ZmodemProcess> {
        let commands = self.zmodem_commands.lock().unwrap().clone();
        if init.upload {
            let selector = self.upload_selector.lock().unwrap().clone();
            let default_path = self.default_upload_path.lock().unwrap().clone();
            let Some(paths) = selector
                .as_ref()
                .map(|selector| {
                    selector(false, default_path).map_err(|error| io::Error::other(error.message))
                })
                .transpose()?
                .flatten()
            else {
                return Err(io::Error::other(
                    "ZMODEM upload cancelled or no upload selector configured",
                ));
            };
            if paths.is_empty() {
                return Err(io::Error::other("ZMODEM upload has no selected files"));
            }
            check_paths_readable(&paths, false).map_err(|error| io::Error::other(error.message))?;
            features::zmodem::spawn_client(commands.0, true, &paths, None)
        } else {
            let selector = self.download_selector.lock().unwrap().clone();
            let default_path = self.default_download_path.lock().unwrap().clone();
            let path = choose_download_path(default_path.as_deref(), selector.as_ref())
                .map_err(|error| io::Error::other(error.message))?
                .ok_or_else(|| {
                    io::Error::other("ZMODEM download cancelled or no download selector configured")
                })?;
            check_path_writable(&path).map_err(|error| io::Error::other(error.message))?;
            features::zmodem::spawn_client(commands.1, false, &[], Some(&path))
        }
    }

    fn start_transfer(
        &self,
        trigger: TrzszTrigger,
        sender: SyncSender<PumpEvent>,
        tmux_ack: features::tmuxcc::TmuxAck,
    ) -> io::Result<SyncSender<Vec<u8>>> {
        if self
            .transferring
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a transfer is already active",
            ));
        }
        let transfer_writer: Box<dyn Write + Send> = if trigger.tmux_pane_id.is_empty() {
            Box::new(self.server_in.writer())
        } else {
            Box::new(features::tmuxcc::TmuxControlWriter::new(
                self.server_in.writer(),
                trigger.tmux_pane_id.clone(),
                tmux_ack,
            ))
        };
        let mut transfer = TrzszTransfer::new(transfer_writer);
        transfer.set_tmux_integration(!trigger.tmux_pane_id.is_empty());
        let buffer_sender = transfer.buffer.sender();
        *self.active_controller.lock().unwrap() = Some(StopPromptController::new(&transfer));
        let state_callback = self.state_callback.lock().unwrap().clone();
        let redraw_callback = self.redraw_callback.lock().unwrap().clone();
        let observer = self.progress_observer.lock().unwrap().clone();
        let pending = if matches!(trigger.mode, 'R' | 'D') {
            self.pending_upload.lock().unwrap().take()
        } else {
            None
        };
        let (upload_paths, upload_result) = match pending {
            Some(request) => (Some(request.paths), request.result),
            None => (None, None),
        };
        let defaults = (
            self.default_upload_path.lock().unwrap().clone(),
            self.default_download_path.lock().unwrap().clone(),
        );
        let selectors = (
            self.upload_selector.lock().unwrap().clone(),
            self.download_selector.lock().unwrap().clone(),
        );
        let client_out = self.client_out.clone();
        let columns = self.columns.clone();
        let transferring = self.transferring.clone();
        let controller_state = self.active_controller.clone();
        let upload_result_for_error = upload_result.clone();
        let spawn_result = thread::Builder::new()
            .name("trzsz-filter-transfer".to_string())
            .spawn(move || {
                if let Some(callback) = state_callback.as_ref() {
                    callback(true);
                }
                let result = handle_transfer(
                    transfer,
                    trigger,
                    upload_paths,
                    defaults,
                    selectors,
                    client_out,
                    observer,
                    columns,
                );
                transferring.store(false, Ordering::SeqCst);
                *controller_state.lock().unwrap() = None;
                if let Some(callback) = state_callback.as_ref() {
                    callback(false);
                }
                if let Some(callback) = redraw_callback.as_ref() {
                    callback();
                }
                if let Some(done) = upload_result {
                    let _ = done.send(result);
                }
                let _ = sender.send(PumpEvent::TransferDone);
            });
        if let Err(error) = spawn_result {
            self.transferring.store(false, Ordering::SeqCst);
            *self.active_controller.lock().unwrap() = None;
            *self.current_trigger.lock().unwrap() = None;
            if let Some(done) = upload_result_for_error {
                let _ = done.send(Err(error.to_string()));
            }
            return Err(io::Error::other(error));
        }
        Ok(buffer_sender)
    }

    fn finish_runtime(&self) {
        self.close();
        self.server_in.close();
        self.client_out.close();
        if let Some(PendingUpload {
            result: Some(done), ..
        }) = self.pending_upload.lock().unwrap().take()
        {
            let _ = done.send(Err("Filter closed before upload started".to_string()));
        }
    }

    fn call_shutdown_handlers(&self) {
        if let Some(callback) = self.client_input_shutdown.lock().unwrap().as_ref() {
            callback();
        }
        if let Some(callback) = self.server_output_shutdown.lock().unwrap().as_ref() {
            callback();
        }
    }

    /// Return the most recently detected transfer trigger, if any.
    pub fn current_trigger(&self) -> Option<TrzszTrigger> {
        self.current_trigger.lock().unwrap().clone()
    }

    /// Alias for [`set_terminal_columns`](Self::set_terminal_columns).
    pub fn set_columns(&self, columns: i32) {
        self.set_terminal_columns(columns);
    }
}

fn spawn_reader<R: Read + Send + 'static>(
    mut reader: R,
    sender: SyncSender<PumpEvent>,
    client: bool,
) -> io::Result<()> {
    thread::Builder::new()
        .name(if client {
            "trzsz-filter-client-input".to_string()
        } else {
            "trzsz-filter-server-output".to_string()
        })
        .spawn(move || {
            let mut buffer = [0u8; 32 * 1024];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let event = if client {
                            PumpEvent::ClientEof
                        } else {
                            PumpEvent::ServerEof
                        };
                        let _ = sender.send(event);
                        return;
                    }
                    Ok(count) => {
                        let event = if client {
                            PumpEvent::ClientData(buffer[..count].to_vec())
                        } else {
                            PumpEvent::ServerData(buffer[..count].to_vec())
                        };
                        if sender.send(event).is_err() {
                            return;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        let event = if client {
                            PumpEvent::ClientError(error)
                        } else {
                            PumpEvent::ServerError(error)
                        };
                        let _ = sender.send(event);
                        return;
                    }
                }
            }
        })?;
    Ok(())
}

fn pump_zmodem_stdout<R: Read + Send + 'static>(mut reader: R, sender: SyncSender<PumpEvent>) {
    let mut buffer = [0u8; 32 * 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                if sender
                    .send(PumpEvent::ZmodemData(buffer[..count].to_vec()))
                    .is_err()
                {
                    return;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    let _ = sender.send(PumpEvent::ZmodemDone);
}

fn make_upload_request(
    paths: &[PathBuf],
    result: Option<mpsc::Sender<Result<(), String>>>,
) -> Result<PendingUpload, TrzszError> {
    if paths.is_empty() {
        return Err(comm::simple_error("nothing to upload"));
    }
    let directory = paths.iter().any(|path| path.is_dir());
    check_paths_readable(paths, directory)?;
    Ok(PendingUpload {
        paths: paths.to_vec(),
        result,
    })
}

fn handle_transfer(
    mut transfer: TrzszTransfer,
    trigger: TrzszTrigger,
    pending_paths: Option<Vec<PathBuf>>,
    defaults: (Option<PathBuf>, Option<PathBuf>),
    selectors: (Option<UploadSelector>, Option<DownloadSelector>),
    client_out: SharedEndpoint,
    observer: Option<ProgressObserver>,
    columns: Arc<std::sync::atomic::AtomicI32>,
) -> Result<(), String> {
    let outcome = match trigger.mode {
        'S' => download_files(
            &mut transfer,
            &trigger,
            defaults.1.as_deref(),
            selectors.1.as_ref(),
            client_out.clone(),
            observer.clone(),
            columns.clone(),
        ),
        'R' | 'D' => upload_files(
            &mut transfer,
            &trigger,
            pending_paths,
            defaults.0.clone(),
            selectors.0.as_ref(),
            client_out.clone(),
            observer,
            columns,
        ),
        _ => Err(comm::simple_error("Unsupported transfer mode")),
    };
    match outcome {
        Ok(message) if message == "Cancelled" => Err("Cancelled".to_string()),
        Ok(message) => {
            let mut output = client_out.writer();
            output
                .write_all(format!("\r\n{}\r\n", message).as_bytes())
                .and_then(|_| output.flush())
                .map_err(|error| error.to_string())?;
            Ok(())
        }
        Err(error) => {
            transfer.client_error(&error);
            let mut output = client_out.writer();
            let _ = output.write_all(format!("\r\n{}\r\n", error.message).as_bytes());
            let _ = output.flush();
            Err(error.message)
        }
    }
}

fn download_files(
    transfer: &mut TrzszTransfer,
    trigger: &TrzszTrigger,
    default_path: Option<&Path>,
    selector: Option<&DownloadSelector>,
    client_out: SharedEndpoint,
    observer: Option<ProgressObserver>,
    columns: Arc<std::sync::atomic::AtomicI32>,
) -> Result<String, TrzszError> {
    let path = choose_download_path(default_path, selector)?;
    let Some(path) = path else {
        transfer.send_action(false, trigger.version.as_ref(), trigger.win_server)?;
        return Ok("Cancelled".to_string());
    };
    check_path_writable(&path)?;
    transfer.send_action(true, trigger.version.as_ref(), trigger.win_server)?;
    let config = transfer.recv_config()?;
    let mut progress = create_progress(
        config.quiet,
        config.tmux_pane_width,
        columns,
        client_out,
        observer,
        &trigger.tmux_prefix,
    );
    let mut callback = Some(&mut progress as &mut dyn ProgressCallback);
    let names = transfer.recv_files(&path, &mut callback)?;
    transfer.recv_exit()?;
    let message = comm::format_saved_files(&names, &path);
    transfer.client_exit(&message)?;
    Ok(message)
}

fn upload_files(
    transfer: &mut TrzszTransfer,
    trigger: &TrzszTrigger,
    pending_paths: Option<Vec<PathBuf>>,
    default_path: Option<PathBuf>,
    selector: Option<&UploadSelector>,
    client_out: SharedEndpoint,
    observer: Option<ProgressObserver>,
    columns: Arc<std::sync::atomic::AtomicI32>,
) -> Result<String, TrzszError> {
    let directory_mode = trigger.mode == 'D';
    let paths = if let Some(paths) = pending_paths {
        paths
    } else if let Some(selector) = selector {
        match selector(directory_mode, default_path)? {
            Some(paths) => paths,
            None => {
                transfer.send_action(false, trigger.version.as_ref(), trigger.win_server)?;
                return Ok("Cancelled".to_string());
            }
        }
    } else {
        transfer.send_action(false, trigger.version.as_ref(), trigger.win_server)?;
        return Ok("Cancelled".to_string());
    };
    if paths.is_empty() {
        transfer.send_action(false, trigger.version.as_ref(), trigger.win_server)?;
        return Ok("Cancelled".to_string());
    }
    let has_directory = paths.iter().any(|path| path.is_dir());
    let files = check_paths_readable(&paths, directory_mode || has_directory)?;
    transfer.send_action(true, trigger.version.as_ref(), trigger.win_server)?;
    let config = transfer.recv_config()?;
    if config.overwrite {
        comm::check_duplicate_names(&files)?;
    }
    let mut progress = create_progress(
        config.quiet,
        config.tmux_pane_width,
        columns,
        client_out,
        observer,
        &trigger.tmux_prefix,
    );
    let mut callback = Some(&mut progress as &mut dyn ProgressCallback);
    let names = transfer.send_files(&files, &mut callback)?;
    let message = comm::format_saved_files(&names, Path::new(""));
    transfer.client_exit(&message)?;
    Ok(message)
}

fn choose_download_path(
    default_path: Option<&Path>,
    selector: Option<&DownloadSelector>,
) -> Result<Option<PathBuf>, TrzszError> {
    if let Some(path) = default_path {
        return Ok(Some(path.to_path_buf()));
    }
    match selector {
        Some(selector) => selector(None),
        None => Ok(None),
    }
}

fn create_progress(
    quiet: bool,
    tmux_pane_width: i32,
    columns: Arc<std::sync::atomic::AtomicI32>,
    client_out: SharedEndpoint,
    observer: Option<ProgressObserver>,
    tmux_prefix: &str,
) -> FilterProgress {
    let bar = if quiet {
        None
    } else {
        let writer: Arc<Mutex<dyn Write + Send>> = Arc::new(Mutex::new(client_out.writer()));
        Some(TextProgressBar::new(
            writer,
            columns.load(Ordering::Relaxed),
            tmux_pane_width,
            tmux_prefix,
        ))
    };
    FilterProgress {
        bar,
        observer,
        event: ProgressEvent::default(),
        columns,
    }
}

struct FilterProgress {
    bar: Option<TextProgressBar>,
    observer: Option<ProgressObserver>,
    event: ProgressEvent,
    columns: Arc<std::sync::atomic::AtomicI32>,
}

impl FilterProgress {
    fn notify(&self) {
        if let Some(observer) = &self.observer {
            observer(self.event.clone());
        }
    }

    fn update_columns(&self) {
        if let Some(bar) = self.bar.as_ref() {
            bar.columns
                .store(self.columns.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }
}

impl Drop for FilterProgress {
    fn drop(&mut self) {
        if let Some(bar) = self.bar.as_ref() {
            bar.show_cursor();
        }
    }
}

impl ProgressCallback for FilterProgress {
    fn on_num(&mut self, num: i64) {
        self.event.file_count = num;
        self.update_columns();
        if let Some(bar) = self.bar.as_mut() {
            bar.on_num(num);
        }
        self.notify();
    }

    fn on_name(&mut self, name: &str) {
        self.event.file_name = name.to_string();
        self.event.file_step = 0;
        self.event.file_size = 0;
        self.event.done = false;
        self.update_columns();
        if let Some(bar) = self.bar.as_mut() {
            bar.on_name(name);
        }
        self.notify();
    }

    fn on_size(&mut self, size: i64) {
        self.event.file_size = size;
        self.update_columns();
        if let Some(bar) = self.bar.as_mut() {
            bar.on_size(size);
        }
        self.notify();
    }

    fn on_step(&mut self, step: i64) {
        self.event.file_step = step;
        self.update_columns();
        if let Some(bar) = self.bar.as_mut() {
            bar.on_step(step);
        }
        self.notify();
    }

    fn on_done(&mut self) {
        self.event.done = true;
        self.update_columns();
        if let Some(bar) = self.bar.as_mut() {
            bar.on_done();
        }
        self.notify();
    }

    fn set_pre_size(&mut self, size: i64) {
        if let Some(bar) = self.bar.as_mut() {
            bar.set_pre_size(size);
        }
    }

    fn set_pause(&mut self, pausing: bool) {
        self.event.pausing = pausing;
        if let Some(bar) = self.bar.as_mut() {
            bar.set_pause(pausing);
        }
        self.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn trigger_detection_parses_and_rewrites_complete_marker() {
        let line = b"shell::TRZSZ:TRANSFER:S:1.2.0:1234567890123:0\r\n";
        let (output, trigger) = TrzszFilter::detect_trzsz(line);
        assert!(String::from_utf8_lossy(&output).contains("::TRZSZGO:TRANSFER:S"));
        let trigger = trigger.unwrap();
        assert_eq!(trigger.mode, 'S');
        assert_eq!(trigger.version, TrzszVersion::parse("1.2.0"));
        assert_eq!(trigger.unique_id, "1234567890123");
    }

    #[test]
    fn trigger_detection_ignores_malformed_and_handles_fragments() {
        let mut detector = TriggerDetector::new();
        let (out, trigger, _) = detector.push(b"ordinary ::TRZSZ:TRANS");
        assert_eq!(out, b"ordinary ");
        assert!(trigger.is_none());
        let (out, trigger, _) = detector.push(b"FER:S:bad:uid\n");
        assert_eq!(out, b"::TRZSZ:TRANSFER:S:bad:uid\n");
        assert!(trigger.is_none());

        let (out, trigger, _) = detector.push(b"::TRZSZ:TRANSFER:R:1.2.0:1234567890123:0\r\n");
        assert!(trigger.is_some());
        assert!(String::from_utf8_lossy(&out).contains("TRZSZGO"));
        let (out, trigger, _) = detector.push(b"noise::TRZSZ:TRANSFER:R:1.2.");
        assert_eq!(out, b"noise");
        assert!(trigger.is_none());
        let (out, trigger, _) = detector.push(b"0:1234567890123:0\r\n");
        assert!(trigger.is_some());
        assert!(String::from_utf8_lossy(&out).contains("TRZSZGO"));
    }

    #[test]
    fn input_pump_forwards_idle_bytes_and_server_output() {
        let input = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::new(Mutex::new(Vec::new()));
        let filter = TrzszFilter::new(
            Box::new(Cursor::new(b"command\r".to_vec())),
            Box::new(SharedWriter(output.clone())),
            Box::new(SharedWriter(input.clone())),
            Box::new(Cursor::new(b"hello".to_vec())),
            TrzszOptions::default(),
        );
        filter.run().unwrap();
        assert_eq!(&*input.lock().unwrap(), b"command\r");
        assert_eq!(&*output.lock().unwrap(), b"hello");
    }

    #[test]
    fn osc52_uses_configured_clipboard_callback_and_preserves_terminal_output() {
        let clipboard = Arc::new(Mutex::new(Vec::new()));
        let captured = clipboard.clone();
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut options = TrzszOptions::default();
        options.enable_osc52 = true;
        let filter = TrzszFilter::new(
            Box::new(Cursor::new(Vec::<u8>::new())),
            Box::new(SharedWriter(output.clone())),
            Box::new(std::io::sink()),
            Box::new(Cursor::new(
                b"before\x1b]52;c;Y29weSB0ZXh0\x07after".to_vec(),
            )),
            options,
        );
        filter.set_clipboard_callback(move |text| captured.lock().unwrap().push(text));
        filter.run().unwrap();
        assert_eq!(&*clipboard.lock().unwrap(), &["copy text"]);
        assert_eq!(
            &*output.lock().unwrap(),
            b"before\x1b]52;c;Y29weSB0ZXh0\x07after"
        );
    }

    #[test]
    fn bracketed_drag_path_dispatches_upload_without_forwarding_path_text() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("dragged.txt");
        std::fs::write(&source, b"payload").unwrap();
        let paste = format!("\x1b[200~'{}'\x1b[201~\r", source.display());
        let mut options = TrzszOptions::default();
        options.detect_drag_file = true;
        let filter = TrzszFilter::new(
            Box::new(Cursor::new(paste.into_bytes())),
            Box::new(std::io::sink()),
            Box::new(SharedWriter(commands.clone())),
            Box::new(Cursor::new(Vec::<u8>::new())),
            options,
        );
        filter.run().unwrap();
        assert_eq!(&*commands.lock().unwrap(), b"\x03trz\r");
    }

    #[test]
    fn upload_request_validates_paths() {
        assert_eq!(
            make_upload_request(&[], None).err().unwrap().message,
            "nothing to upload"
        );
    }

    #[test]
    fn trigger_mode_flags_are_parsed_safely() {
        assert!(parse_trigger(b"::TRZSZ:TRANSFER:x:1.2.0:123:0\n").is_none());
        let trigger = parse_trigger(b"::TRZSZ:TRANSFER:R:1.2.0:1:7000\n").unwrap();
        assert!(parse_trigger(b"::TRZSZ:TRANSFER:R:1.2.0:invalid:0\n").is_none());
        assert!(parse_trigger(b"::TRZSZ:TRANSFER:R:1.2.0:123:port\n").is_none());
        assert!(parse_trigger(b"::TRZSZ:TRANSFER:R:1.2.0:123:-1\n").is_none());
        assert!(trigger.win_server);
        assert_eq!(trigger.tunnel_port, 7000);
        let relay_trigger = parse_trigger(b"::TRZSZ:TRANSFER:R:1.2.0:123:7000#R\n").unwrap();
        assert_eq!(relay_trigger.tunnel_port, 7000);
    }
    struct ShutdownReader(mpsc::Receiver<()>);

    impl Read for ShutdownReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            let _ = self.0.recv();
            Ok(0)
        }
    }

    struct ErrorReader;

    impl Read for ErrorReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("injected reader failure"))
        }
    }

    #[test]
    fn close_wakes_controlled_readers_and_ends_run() {
        let (client_shutdown, client_wait) = mpsc::channel();
        let (server_shutdown, server_wait) = mpsc::channel();
        let filter = Arc::new(TrzszFilter::new(
            Box::new(ShutdownReader(client_wait)),
            Box::new(std::io::sink()),
            Box::new(std::io::sink()),
            Box::new(ShutdownReader(server_wait)),
            TrzszOptions::default(),
        ));
        let wake_client: ShutdownCallback = Arc::new(move || {
            let _ = client_shutdown.send(());
        });
        let wake_server: ShutdownCallback = Arc::new(move || {
            let _ = server_shutdown.send(());
        });
        filter.set_shutdown_handlers(Some(wake_client), Some(wake_server));
        let running = filter.clone();
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = done_tx.send(running.run());
        });

        thread::sleep(Duration::from_millis(20));
        filter.close();

        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .is_ok()
        );
    }

    #[test]
    fn reader_io_error_is_returned_and_other_pump_is_shutdown() {
        let (server_shutdown, server_wait) = mpsc::channel();
        let filter = Arc::new(TrzszFilter::new(
            Box::new(ErrorReader),
            Box::new(std::io::sink()),
            Box::new(std::io::sink()),
            Box::new(ShutdownReader(server_wait)),
            TrzszOptions::default(),
        ));
        let wake_server: ShutdownCallback = Arc::new(move || {
            let _ = server_shutdown.send(());
        });
        filter.set_shutdown_handlers(None, Some(wake_server));

        assert_eq!(
            filter.run().unwrap_err().to_string(),
            "injected reader failure"
        );
    }
    struct ErrorWriter;

    impl Write for ErrorWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected writer failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn client_output_io_error_is_returned() {
        let filter = TrzszFilter::new(
            Box::new(Cursor::new(Vec::<u8>::new())),
            Box::new(ErrorWriter),
            Box::new(std::io::sink()),
            Box::new(Cursor::new(b"server output".to_vec())),
            TrzszOptions::default(),
        );

        assert_eq!(
            filter.run().unwrap_err().to_string(),
            "injected writer failure"
        );
    }

    #[test]
    fn active_upload_api_queues_paths_and_starts_remote_trz() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let (client_shutdown, client_wait) = mpsc::channel();
        let (server_shutdown, server_wait) = mpsc::channel();
        let filter = Arc::new(TrzszFilter::new(
            Box::new(ShutdownReader(client_wait)),
            Box::new(std::io::sink()),
            Box::new(SharedWriter(commands.clone())),
            Box::new(ShutdownReader(server_wait)),
            TrzszOptions::default(),
        ));
        let wake_client: ShutdownCallback = Arc::new(move || {
            let _ = client_shutdown.send(());
        });
        let wake_server: ShutdownCallback = Arc::new(move || {
            let _ = server_shutdown.send(());
        });
        filter.set_shutdown_handlers(Some(wake_client), Some(wake_server));
        let running = filter.clone();
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = done_tx.send(running.run());
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !filter.running.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }

        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        std::fs::write(&source, b"source").unwrap();
        filter.upload_files(&[source]).unwrap();
        assert_eq!(&*commands.lock().unwrap(), b"\x03trz\r");

        filter.close();
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .is_ok()
        );
    }
}
