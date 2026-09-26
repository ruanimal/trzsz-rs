use std::path::PathBuf;
use std::time::Instant;

const MAX_DRAG_INPUT: usize = 64 * 1024;

#[derive(Default)]
pub(in crate::filter) struct DragInput {
    pending: Vec<u8>,
    since: Option<Instant>,
}

impl DragInput {
    /// Buffer input that might be a pasted absolute path. `true` means the
    /// bytes are being held until a delimiter or an idle timeout resolves it.
    pub fn push(&mut self, bytes: &[u8], now: Instant) -> bool {
        if self.pending.is_empty() && !may_start_drag(bytes) {
            return false;
        }
        self.pending.extend_from_slice(bytes);
        self.since = Some(now);
        true
    }

    pub fn is_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn should_finish(&self, now: Instant) -> bool {
        self.pending.contains(&b'\r')
            || self.pending.contains(&b'\n')
            || self
                .since
                .is_some_and(|since| now.duration_since(since).as_millis() >= 300)
            || self.pending.len() > MAX_DRAG_INPUT
    }

    /// Resolve the buffered bytes, returning detected paths or the original
    /// bytes to forward. Invalid or incomplete input is never swallowed.
    pub fn finish(&mut self) -> (Vec<u8>, Option<Vec<PathBuf>>) {
        let bytes = std::mem::take(&mut self.pending);
        self.since = None;
        let paths = parse_drag_paths(&bytes);
        (bytes, paths)
    }
}

fn may_start_drag(bytes: &[u8]) -> bool {
    let mut input = trim_ascii_start(bytes);
    for prefix in [
        b"\x1b[200~".as_slice(),
        b"\x10".as_slice(),
        b"\x1bi\x10".as_slice(),
    ] {
        if input.starts_with(prefix) {
            input = &input[prefix.len()..];
            break;
        }
        if prefix.starts_with(input) && !input.is_empty() {
            return true;
        }
    }
    #[cfg(windows)]
    if windows_drive_path_prefix(input) {
        return true;
    }
    input
        .first()
        .is_some_and(|byte| matches!(byte, b'/' | 0x27 | b'"' | 0x1b))
}

#[cfg(any(windows, test))]
fn windows_drive_path_prefix(input: &[u8]) -> bool {
    (input.len() == 2 && input[0].is_ascii_alphabetic() && input[1] == b':')
        || (input.len() >= 3
            && input[0].is_ascii_alphabetic()
            && input[1] == b':'
            && matches!(input[2], b'\\' | b'/'))
}
fn trim_ascii_start(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    bytes
}

/// Parse POSIX shell-quoted absolute paths and validate that each path is an
/// existing regular file or directory. Bracketed-paste and Warp wrappers are
/// stripped before shell tokenization.
pub fn parse_drag_paths(input: &[u8]) -> Option<Vec<PathBuf>> {
    let mut bytes = trim_ascii_start(input);
    bytes = bytes.strip_suffix(b"\r\n").unwrap_or(bytes);
    bytes = bytes
        .strip_suffix(b"\r")
        .or_else(|| bytes.strip_suffix(b"\n"))
        .unwrap_or(bytes);
    if let Some(rest) = bytes.strip_prefix(b"\x1b[200~") {
        bytes = rest.strip_suffix(b"\x1b[201~").unwrap_or(rest);
    }
    if let Some(rest) = bytes.strip_prefix(b"\x1b[201~") {
        bytes = rest;
    }
    if let Some(rest) = bytes.strip_prefix(b"\x1bi\x10") {
        bytes = rest;
    }
    if let Some(rest) = bytes.strip_prefix(b"\x10") {
        bytes = rest;
    }
    let text = std::str::from_utf8(bytes).ok()?.trim();
    if text.is_empty() {
        return None;
    }
    let words = shell_words(text)?;
    if words.is_empty() {
        return None;
    }
    let mut paths = Vec::with_capacity(words.len());
    for word in words {
        if word.len() < 2 {
            return None;
        }
        let path = normalized_drag_path(&word)?;
        let metadata = std::fs::metadata(&path).ok()?;
        if !metadata.is_file() && !metadata.is_dir() {
            return None;
        }
        paths.push(path);
    }
    Some(paths)
}

fn normalized_drag_path(word: &str) -> Option<PathBuf> {
    #[cfg(windows)]
    {
        if is_windows_drive_path(word) {
            return Some(PathBuf::from(word));
        }
        if let Some(path) = cygwin_path_to_windows(word).or_else(|| msys_path_to_windows(word)) {
            return Some(PathBuf::from(path));
        }
        let output = std::process::Command::new("cygpath")
            .args(["-w", word])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let path = String::from_utf8(output.stdout).ok()?;
        let path = path.trim();
        is_windows_drive_path(path).then(|| PathBuf::from(path))
    }
    #[cfg(not(windows))]
    {
        word.starts_with('/').then(|| PathBuf::from(word))
    }
}

#[cfg(any(windows, test))]
fn is_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/')
}

#[cfg(any(windows, test))]
fn cygwin_path_to_windows(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/cygdrive/")?;
    msys_drive_path_to_windows(rest)
}

#[cfg(any(windows, test))]
fn msys_path_to_windows(path: &str) -> Option<String> {
    msys_drive_path_to_windows(path.strip_prefix('/')?)
}

#[cfg(any(windows, test))]
fn msys_drive_path_to_windows(path: &str) -> Option<String> {
    let (drive, rest) = path.split_once('/')?;
    if drive.len() != 1 || !drive.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    Some(format!("{drive}:\\{}", rest.replace('/', "\\")))
}

fn shell_words(text: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut chars = text.chars().peekable();
    let mut quote = None;
    let mut started = false;
    while let Some(ch) = chars.next() {
        match quote {
            Some(quote_char) => {
                if ch == quote_char {
                    quote = None;
                } else if quote_char == '"' && ch == '\\' {
                    let next = chars.next()?;
                    if matches!(next, '"' | '\\') {
                        word.push(next);
                    } else {
                        word.push('\\');
                        word.push(next);
                    }
                } else {
                    word.push(ch);
                }
            }
            None => match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    started = true;
                }
                '\\' => {
                    let next = chars.next()?;
                    if next.is_whitespace() || matches!(next, '\'' | '"') {
                        word.push(next);
                    } else {
                        word.push('\\');
                        word.push(next);
                    }
                    started = true;
                }
                ch if ch.is_whitespace() => {
                    if started {
                        words.push(std::mem::take(&mut word));
                        started = false;
                    }
                }
                _ => {
                    word.push(ch);
                    started = true;
                }
            },
        }
    }
    if quote.is_some() {
        return None;
    }
    if started {
        words.push(word);
    }
    Some(words)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_existing_shell_quoted_and_bracketed_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("has space.txt");
        std::fs::write(&path, b"data").unwrap();
        let quoted = format!("'{}'", path.display());
        let paste = format!("\x1b[200~{}\x1b[201~", quoted);
        assert_eq!(parse_drag_paths(paste.as_bytes()), Some(vec![path.clone()]));
        assert_eq!(
            parse_drag_paths(format!("\x10{}", quoted).as_bytes()),
            Some(vec![path.clone()])
        );
        assert_eq!(
            parse_drag_paths(format!("\x1bi\x10{}", quoted).as_bytes()),
            Some(vec![path])
        );
    }

    #[test]
    fn rejects_relative_missing_nonregular_and_unclosed_paths() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        std::fs::write(&file, b"x").unwrap();
        assert!(parse_drag_paths(b"relative").is_none());
        assert!(parse_drag_paths(b"'/tmp/unclosed").is_none());
        assert!(parse_drag_paths(b"/this/path/does/not/exist").is_none());
        assert!(parse_drag_paths(file.to_string_lossy().as_bytes()).is_some());
    }

    #[test]
    fn input_detector_safely_retains_fragments_and_flushes_invalid_input() {
        let now = Instant::now();
        let mut input = DragInput::default();
        assert!(input.push(b"'/tmp/part", now));
        assert!(input.push(b"ial'", now));
        assert!(input.is_pending());
        let (bytes, paths) = input.finish();
        assert_eq!(bytes, b"'/tmp/partial'");
        assert!(paths.is_none());
    }
    #[test]
    fn parses_native_msys_and_cygwin_windows_path_forms() {
        assert_eq!(
            shell_words(r#""C:\Users\user\My File.txt""#),
            Some(vec![r"C:\Users\user\My File.txt".to_string()])
        );
        assert_eq!(
            msys_path_to_windows("/c/Users/user/file"),
            Some(r"c:\Users\user\file".to_string())
        );
        assert_eq!(
            cygwin_path_to_windows("/cygdrive/d/Users/user/file"),
            Some(r"d:\Users\user\file".to_string())
        );
        assert!(is_windows_drive_path(r"C:\file.txt"));
    }
    #[test]
    fn windows_drive_prefix_is_held_while_the_separator_has_not_arrived() {
        assert!(windows_drive_path_prefix(b"C:"));
        assert!(windows_drive_path_prefix(&[b'C', b':', 0x5c]));
    }
}
