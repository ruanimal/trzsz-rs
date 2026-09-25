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

use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// Global flag: whether trzsz is affected by Windows behavior.
static AFFECTED_BY_WINDOWS: AtomicBool = AtomicBool::new(false);

pub fn is_affected_by_windows() -> bool {
    AFFECTED_BY_WINDOWS.load(Ordering::Relaxed)
}

pub fn set_affected_by_windows(affected: bool) {
    AFFECTED_BY_WINDOWS.store(affected, Ordering::Relaxed);
}

pub fn is_running_on_windows() -> bool {
    cfg!(target_os = "windows")
}

pub fn is_running_on_macos() -> bool {
    cfg!(target_os = "macos")
}

pub fn is_running_on_linux() -> bool {
    cfg!(target_os = "linux")
}

// ─── Error types ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct TrzszError {
    pub message: String,
    pub err_type: String,
    pub trace: bool,
}

impl fmt::Display for TrzszError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TrzszError {}

impl TrzszError {
    pub fn is_remote_exit(&self) -> bool {
        self.err_type == "EXIT"
    }

    pub fn is_remote_fail(&self) -> bool {
        self.err_type == "fail" || self.err_type == "FAIL"
    }

    pub fn is_stop_and_delete(&self) -> bool {
        self.err_type == "fail" && self.message == ERR_STOPPED_AND_DELETED.message
    }

    pub fn is_trace_back(&self) -> bool {
        if self.err_type == "fail" || self.err_type == "EXIT" {
            false
        } else {
            self.trace
        }
    }
}

pub fn new_trzsz_error(message: &str, err_type: &str, trace: bool) -> TrzszError {
    let mut msg = message.to_string();
    if err_type == "fail" || err_type == "FAIL" || err_type == "EXIT" {
        if let Ok(decoded) = crate::escape::decode_string(&msg) {
            if let Ok(s) = String::from_utf8(decoded) {
                msg = s;
            }
        }
    } else if !err_type.is_empty() {
        msg = format!("[TrzszError] {}: {}", err_type, message);
    }
    TrzszError {
        message: msg,
        err_type: err_type.to_string(),
        trace,
    }
}

pub fn simple_trzsz_error(format: &str, args: impl fmt::Display) -> TrzszError {
    TrzszError {
        message: format!("{}: {}", format, args),
        err_type: String::new(),
        trace: false,
    }
}

pub fn simple_error(format: &str) -> TrzszError {
    TrzszError {
        message: format.to_string(),
        err_type: String::new(),
        trace: false,
    }
}

pub static ERR_STOPPED: std::sync::LazyLock<TrzszError> =
    std::sync::LazyLock::new(|| simple_error("Stopped"));

pub static ERR_STOPPED_AND_DELETED: std::sync::LazyLock<TrzszError> =
    std::sync::LazyLock::new(|| simple_error("Stopped and deleted"));

pub static ERR_RECEIVE_DATA_TIMEOUT: std::sync::LazyLock<TrzszError> =
    std::sync::LazyLock::new(|| simple_error("Receive data timeout"));

pub static ERR_INTERRUPTED: std::sync::LazyLock<TrzszError> =
    std::sync::LazyLock::new(|| simple_error("Interrupted"));

pub fn err_stopped() -> TrzszError {
    simple_error("Stopped")
}

pub fn err_stopped_and_deleted() -> TrzszError {
    simple_error("Stopped and deleted")
}

pub fn err_receive_data_timeout() -> TrzszError {
    simple_error("Receive data timeout")
}

pub fn err_interrupted() -> TrzszError {
    simple_error("Interrupted")
}

pub fn err_user_canceled() -> TrzszError {
    simple_error("Cancelled")
}

// ─── Write all ─────────────────────────────────────────────────────────────

pub fn write_all(writer: &mut impl Write, buf: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        written += writer.write(&buf[written..])?;
    }
    Ok(())
}

// ─── Source / target file types ────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceFile {
    #[serde(rename = "path_id")]
    pub path_id: i32,
    #[serde(skip)]
    pub abs_path: PathBuf,
    #[serde(rename = "path_name")]
    pub rel_path: Vec<String>,
    #[serde(rename = "is_dir")]
    pub is_dir: bool,
    #[serde(default)]
    pub archive: bool,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub perm: Option<u32>,
}

impl SourceFile {
    pub fn get_file_name(&self) -> &str {
        self.rel_path.last().map(|s| s.as_str()).unwrap_or("")
    }

    pub fn marshal(&self) -> Result<String, serde_json::Error> {
        let mut file = self.clone();
        file.archive = !file.sub_files().is_empty();
        serde_json::to_string(&file)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TargetFile {
    pub name: String,
    pub size: i64,
}

// ─── Path validation ───────────────────────────────────────────────────────

pub fn check_path_writable(path: &Path) -> Result<(), TrzszError> {
    let metadata = fs::metadata(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            simple_trzsz_error("No such directory", path.display())
        } else {
            simple_trzsz_error("Stat error", e)
        }
    })?;
    if !metadata.is_dir() {
        return Err(simple_trzsz_error("Not a directory", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if !metadata.permissions().mode() & 0o200 != 0 {
            return Err(simple_trzsz_error("No permission to write", path.display()));
        }
    }
    Ok(())
}

pub fn check_path_readable_single(path: &Path) -> Result<fs::Metadata, TrzszError> {
    let metadata = fs::metadata(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            simple_trzsz_error("No such file", path.display())
        } else {
            simple_trzsz_error("Stat error", e)
        }
    })?;
    Ok(metadata)
}

pub fn check_paths_readable(
    paths: &[PathBuf],
    directory: bool,
) -> Result<Vec<SourceFile>, TrzszError> {
    let mut list = Vec::new();
    for (i, p) in paths.iter().enumerate() {
        let abs_path = fs::canonicalize(p)
            .map_err(|e| simple_trzsz_error(&format!("Canonicalize [{}] error", p.display()), e))?;
        let metadata = fs::metadata(&abs_path).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                simple_trzsz_error("No such file", abs_path.display())
            } else {
                simple_trzsz_error("Stat error", e)
            }
        })?;
        if !directory && metadata.is_dir() {
            return Err(simple_trzsz_error("Is a directory", abs_path.display()));
        }
        // Initial rel_path must contain the file/dir name so the receiver
        // knows what to call the file (matches trzsz-go behavior).
        let initial_name = abs_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let mut visited = std::collections::HashSet::new();
        check_path_readable_recursive(
            i as i32,
            &abs_path,
            &metadata,
            &mut list,
            vec![initial_name],
            &mut visited,
        )?;
    }
    Ok(list)
}

fn check_path_readable_recursive(
    path_id: i32,
    path: &Path,
    metadata: &fs::Metadata,
    list: &mut Vec<SourceFile>,
    rel_path: Vec<String>,
    visited: &mut std::collections::HashSet<PathBuf>,
) -> Result<(), TrzszError> {
    #[cfg(unix)]
    let perm = {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() as u32 & 0o777
    };
    #[cfg(not(unix))]
    let perm = 0u32;

    if !metadata.is_dir() {
        if !metadata.file_type().is_file() {
            return Err(simple_trzsz_error("Not a regular file", path.display()));
        }
        list.push(SourceFile {
            path_id,
            abs_path: path.to_path_buf(),
            rel_path,
            is_dir: false,
            archive: false,
            size: metadata.len() as i64,
            perm: Some(perm),
        });
        return Ok(());
    }

    let real_path = fs::canonicalize(path)
        .map_err(|e| simple_trzsz_error(&format!("EvalSymlinks [{}] error", path.display()), e))?;
    if !visited.insert(real_path.clone()) {
        return Err(simple_trzsz_error("Duplicate link", path.display()));
    }

    list.push(SourceFile {
        path_id,
        abs_path: path.to_path_buf(),
        rel_path: rel_path.clone(),
        is_dir: true,
        archive: false,
        size: 0,
        perm: Some(perm),
    });

    let entries = fs::read_dir(path)
        .map_err(|e| simple_trzsz_error(&format!("Readdir [{}] error", path.display()), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| simple_trzsz_error(&format!("ReadDir entry error"), e))?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        let child_path = entry.path();
        let child_meta = fs::metadata(&child_path).map_err(|e| {
            simple_trzsz_error(&format!("Stat [{}] error", child_path.display()), e)
        })?;
        let mut child_rel = rel_path.clone();
        child_rel.push(file_name);
        check_path_readable_recursive(path_id, &child_path, &child_meta, list, child_rel, visited)?;
    }
    Ok(())
}

pub fn check_duplicate_names(files: &[SourceFile]) -> Result<(), TrzszError> {
    let mut seen = std::collections::HashSet::new();
    for file in files {
        let key = file.rel_path.join("/");
        if !seen.insert(key.clone()) {
            return Err(simple_trzsz_error("Duplicate name", &key));
        }
    }
    Ok(())
}

pub fn get_new_name(path: &Path, name: &str) -> Result<String, TrzszError> {
    const MAX_NAME_LEN: usize = 255;
    if name.len() > MAX_NAME_LEN {
        return Err(simple_trzsz_error("File name too long", name));
    }
    let full_path = path.join(name);
    if !full_path.exists() {
        return Ok(name.to_string());
    }
    for i in 0..1000 {
        let new_name = format!("{}.{}", name, i);
        if !path.join(&new_name).exists() {
            return Ok(new_name);
        }
    }
    Err(simple_trzsz_error("Fail to assign new file name to", name))
}

// ─── Tmux detection ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TmuxMode {
    None,
    Normal,
    Control,
}

pub(crate) fn binary_mode_enabled(
    requested: bool,
    remote_supports_binary: bool,
    tmux_compatible: bool,
    on_windows: bool,
) -> bool {
    requested && remote_supports_binary && tmux_compatible && !on_windows
}

pub fn check_tmux() -> Result<(TmuxMode, Option<String>, i32), TrzszError> {
    if std::env::var("TMUX").is_err() {
        return Ok((TmuxMode::None, None, -1));
    }

    let output = std::process::Command::new("tmux")
        .args([
            "display-message",
            "-p",
            "#{client_tty}:#{client_control_mode}:#{pane_width}",
        ])
        .output()
        .map_err(|e| simple_trzsz_error("Get tmux output failed", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let output_str = stdout.trim();
    let tokens: Vec<&str> = output_str.split(':').collect();
    if tokens.len() != 3 {
        return Err(simple_trzsz_error("Unexpected tmux output", output_str));
    }

    let tmux_tty = tokens[0];
    let control_mode = tokens[1];
    let pane_width_str = tokens[2];

    let tmux_pane_width = if !pane_width_str.is_empty() {
        pane_width_str
            .parse::<i32>()
            .map_err(|e| simple_trzsz_error("Parse tmux pane width failed", e))?
    } else {
        -1
    };

    if control_mode == "1" || !tmux_tty.starts_with('/') {
        return Ok((TmuxMode::Control, None, tmux_pane_width));
    }

    if !Path::new(tmux_tty).exists() {
        return Ok((TmuxMode::Control, None, tmux_pane_width));
    }

    Ok((
        TmuxMode::Normal,
        Some(tmux_tty.to_string()),
        tmux_pane_width,
    ))
}

pub fn get_terminal_columns() -> i32 {
    let output = std::process::Command::new("stty")
        .arg("size")
        .stdin(std::process::Stdio::inherit())
        .output();
    match output {
        Ok(o) => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            let parts: Vec<&str> = stdout.trim().split(' ').collect();
            if parts.len() == 2 {
                parts[1].parse::<i32>().unwrap_or(0)
            } else {
                0
            }
        }
        Err(_) => 0,
    }
}

pub fn tmux_refresh_client() {
    let _ = std::process::Command::new("tmux")
        .arg("refresh-client")
        .output();
}

// ─── VT100 helpers ─────────────────────────────────────────────────────────

pub fn is_vt100_end(b: u8) -> bool {
    (b >= b'a' && b <= b'z') || (b >= b'A' && b <= b'Z')
}

pub fn trim_vt100(buf: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(buf.len());
    let mut skip_vt100 = false;
    for &b in buf {
        if skip_vt100 {
            if is_vt100_end(b) {
                skip_vt100 = false;
            }
        } else if b == 0x1b {
            skip_vt100 = true;
        } else {
            result.push(b);
        }
    }
    result
}

pub fn show_cursor(writer: &mut impl Write) {
    let _ = writer.write_all(b"\x1b[?25h");
}

pub fn hide_cursor(writer: &mut impl Write) {
    let _ = writer.write_all(b"\x1b[?25l");
}

// ─── Format saved files ────────────────────────────────────────────────────

pub fn format_saved_files(names: &[String], path: &Path) -> String {
    if names.is_empty() {
        return "No file saved".to_string();
    }
    let count = names.len();
    let plural = if count > 1 {
        "files/directories"
    } else {
        "file/directory"
    };
    let mut result = format!("Saved {} {}", count, plural);
    let display_path = path.to_string_lossy();
    if !display_path.is_empty() {
        result.push_str(" to ");
        result.push_str(&display_path);
    }
    for name in names {
        result.push_str("\r\n- ");
        result.push_str(name);
    }
    result
}

// ─── Join file names ───────────────────────────────────────────────────────

pub fn join_file_names(msg: &str, files: &[String]) -> String {
    format!("{}\n{}", msg, files.join("\n"))
}

// ─── Read all from reader ──────────────────────────────────────────────────

pub fn read_all_string(reader: &mut impl io::Read) -> io::Result<String> {
    let mut buf = String::new();
    reader.read_to_string(&mut buf)?;
    Ok(buf)
}

// ─── Compress type ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(i32)]
pub enum CompressType {
    Auto = 0,
    Yes = 1,
    No = 2,
}

impl CompressType {
    pub fn from_str(s: &str) -> Result<Self, TrzszError> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(CompressType::Auto),
            "yes" => Ok(CompressType::Yes),
            "no" => Ok(CompressType::No),
            _ => Err(simple_trzsz_error("Invalid compress type", s)),
        }
    }
}

impl Default for CompressType {
    fn default() -> Self {
        CompressType::Auto
    }
}

// ─── Buffer size ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct BufferSize {
    pub size: i64,
}

impl BufferSize {
    pub fn parse(s: &str) -> Result<Self, TrzszError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(simple_trzsz_error("Invalid size", s));
        }
        let (num_part, unit_part) =
            s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
        let size_value = num_part
            .parse::<i64>()
            .map_err(|_| simple_trzsz_error("Invalid size", s))?;
        let size = match unit_part.to_lowercase().as_str() {
            "" | "b" => size_value,
            "k" | "kb" => size_value * 1024,
            "m" | "mb" => size_value * 1024 * 1024,
            "g" | "gb" => size_value * 1024 * 1024 * 1024,
            _ => return Err(simple_trzsz_error("Invalid size", s)),
        };
        if size < 1024 {
            return Err(simple_error("Less than 1K"));
        }
        if size > 1024 * 1024 * 1024 {
            return Err(simple_error("Greater than 1G"));
        }
        Ok(BufferSize { size })
    }
}

impl Default for BufferSize {
    fn default() -> Self {
        BufferSize {
            size: 10 * 1024 * 1024,
        }
    }
}

impl fmt::Display for BufferSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.size)
    }
}

// ─── File writer trait ─────────────────────────────────────────────────────

pub trait FileWriter {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    fn close(&mut self) -> io::Result<()>;
}

pub struct SimpleFileWriter {
    pub file: fs::File,
}

impl FileWriter for SimpleFileWriter {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.file.write_all(buf)
    }
    fn close(&mut self) -> io::Result<()> {
        // The transfer-owned writer drops immediately after close returns.
        Ok(())
    }
}

pub trait FileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    fn size(&self) -> i64;
    fn close(&mut self) -> io::Result<()>;
    fn seek(&mut self, _pos: io::SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file reader is not seekable",
        ))
    }
}

pub struct SimpleFileReader {
    pub file: fs::File,
    pub file_size: i64,
}

impl FileReader for SimpleFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use std::io::Read;
        self.file.read(buf)
    }
    fn size(&self) -> i64 {
        self.file_size
    }
    fn close(&mut self) -> io::Result<()> {
        // The transfer-owned reader drops immediately after close returns.
        Ok(())
    }
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        use std::io::Seek;
        self.file.seek(pos)
    }
}

// ─── Archive source file support ───────────────────────────────────────────

pub trait SourceFileExt {
    fn sub_files(&self) -> &[SourceFile];
}

impl SourceFile {
    pub fn sub_files(&self) -> &[SourceFile] {
        &[]
    }
}

// ─── Tmux status interval ──────────────────────────────────────────────────

pub fn get_tmux_status_interval() -> String {
    let output = std::process::Command::new("tmux")
        .args(["display-message", "-p", "#{status-interval}"])
        .output();
    match output {
        Ok(o) => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            let trimmed = stdout.trim();
            if trimmed.is_empty() {
                "15".to_string()
            } else {
                trimmed.to_string()
            }
        }
        Err(_) => "15".to_string(),
    }
}

pub fn set_tmux_status_interval(interval: &str) {
    let interval = if interval.is_empty() { "15" } else { interval };
    let _ = std::process::Command::new("tmux")
        .args(["setw", "status-interval", interval])
        .output();
}

// ─── Listen for tunnel ─────────────────────────────────────────────────────

pub fn listen_for_tunnel() -> (Option<std::net::TcpListener>, i32) {
    match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            let port = listener.local_addr().map(|a| a.port() as i32).unwrap_or(0);
            let _ = listener.set_nonblocking(false);
            (Some(listener), port)
        }
        Err(_) => (None, 0),
    }
}

// ─── Fork to background (Unix only) ────────────────────────────────────────

#[cfg(unix)]
pub fn fork_to_background() -> Result<bool, TrzszError> {
    use nix::unistd::{ForkResult, fork};
    match unsafe { fork() } {
        Ok(ForkResult::Parent { .. }) => Ok(true),
        Ok(ForkResult::Child) => {
            // Detach from terminal
            let _ = nix::unistd::setsid();
            Ok(false)
        }
        Err(e) => Err(simple_trzsz_error("Fork failed", e)),
    }
}

#[cfg(not(unix))]
pub fn fork_to_background() -> Result<bool, TrzszError> {
    Err(simple_error(
        "Fork to background is not supported on this platform",
    ))
}

// ─── Running on tmux ───────────────────────────────────────────────────────

pub fn is_warp_terminal() -> bool {
    std::env::var("TERM_PROGRAM")
        .map(|v| v == "WarpTerminal")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_size_parse() {
        assert_eq!(BufferSize::parse("1K").unwrap().size, 1024);
        assert_eq!(BufferSize::parse("1024").unwrap().size, 1024);
        assert_eq!(BufferSize::parse("1M").unwrap().size, 1024 * 1024);
        assert_eq!(BufferSize::parse("1MB").unwrap().size, 1024 * 1024);
        assert_eq!(BufferSize::parse("1m").unwrap().size, 1024 * 1024);
        assert_eq!(BufferSize::parse("1G").unwrap().size, 1024 * 1024 * 1024);
        assert_eq!(BufferSize::parse("1GB").unwrap().size, 1024 * 1024 * 1024);
        assert_eq!(BufferSize::parse("2K").unwrap().size, 2 * 1024);
        assert_eq!(BufferSize::parse("10").unwrap_err().message, "Less than 1K");
        assert_eq!(
            BufferSize::parse("2GB").unwrap_err().message,
            "Greater than 1G"
        );
    }

    #[test]
    fn test_binary_mode_platform_fallbacks() {
        assert!(binary_mode_enabled(true, true, true, false));
        assert!(!binary_mode_enabled(true, true, false, false));
        assert!(!binary_mode_enabled(true, true, true, true));
        assert!(!binary_mode_enabled(false, true, true, false));
        assert!(!binary_mode_enabled(true, false, true, false));
    }

    #[test]
    fn test_compress_type() {
        assert_eq!(CompressType::from_str("auto").unwrap(), CompressType::Auto);
        assert_eq!(CompressType::from_str("yes").unwrap(), CompressType::Yes);
        assert_eq!(CompressType::from_str("no").unwrap(), CompressType::No);
        assert_eq!(CompressType::from_str("AUTO").unwrap(), CompressType::Auto);
        assert_eq!(CompressType::from_str("YES").unwrap(), CompressType::Yes);
        assert!(CompressType::from_str("invalid").is_err());
    }

    #[test]
    fn test_trim_vt100() {
        assert_eq!(trim_vt100(b"hello"), b"hello");
        assert_eq!(trim_vt100(b"\x1b[2J"), b"");
        assert_eq!(trim_vt100(b"ABC\x1b[2JDEF"), b"ABCDEF");
    }

    #[test]
    fn test_get_new_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        assert_eq!(get_new_name(path, "test.txt").unwrap(), "test.txt");
        fs::write(path.join("test.txt"), "").unwrap();
        assert_eq!(get_new_name(path, "test.txt").unwrap(), "test.txt.0");
        fs::write(path.join("test.txt.0"), "").unwrap();
        assert_eq!(get_new_name(path, "test.txt").unwrap(), "test.txt.1");
    }

    #[test]
    fn test_format_saved_files() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(format_saved_files(&[], dir.path()), "No file saved");
        assert_eq!(
            format_saved_files(&["foo.txt".to_string()], dir.path()),
            format!(
                "Saved 1 file/directory to {}\r\n- foo.txt",
                dir.path().display()
            )
        );
        assert_eq!(
            format_saved_files(&["a.txt".to_string(), "b.txt".to_string()], dir.path()),
            format!(
                "Saved 2 files/directories to {}\r\n- a.txt\r\n- b.txt",
                dir.path().display()
            )
        );
    }
}
