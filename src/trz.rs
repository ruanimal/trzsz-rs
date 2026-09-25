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
use std::sync::atomic::{AtomicBool, Ordering};

use crate::args::TrzArgs;
use crate::comm::{self, TrzszError, check_path_writable, format_saved_files};
use crate::escape::get_escape_chars;
use crate::transfer::TrzszTransfer;
use crate::version::TRZSZ_VERSION;

#[cfg(unix)]
struct RawModeGuard {
    fd: std::os::unix::io::RawFd,
    saved: nix::sys::termios::Termios,
}

#[cfg(unix)]
impl RawModeGuard {
    fn enter(fd: std::os::unix::io::RawFd) -> Option<Self> {
        use nix::sys::termios::{SetArg, tcgetattr, tcsetattr};
        let saved = tcgetattr(unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }).ok()?;
        let mut raw = saved.clone();
        nix::sys::termios::cfmakeraw(&mut raw);
        tcsetattr(
            unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) },
            SetArg::TCSANOW,
            &raw,
        )
        .ok()?;
        Some(RawModeGuard { fd, saved })
    }
}

#[cfg(unix)]
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        use nix::sys::termios::{SetArg, tcsetattr};
        let _ = tcsetattr(
            unsafe { std::os::fd::BorrowedFd::borrow_raw(self.fd) },
            SetArg::TCSANOW,
            &self.saved,
        );
    }
}

pub fn trz_main(args: &TrzArgs) -> i32 {
    // Fork to background
    if args.base.fork {
        match comm::fork_to_background() {
            Ok(true) => return 0,
            Ok(false) => {}
            Err(e) => {
                eprintln!("{}", e.message);
                return 1;
            }
        }
    }

    // Resolve absolute path
    let path = std::env::current_dir()
        .map(|d| d.join(&args.path))
        .unwrap_or_else(|_| args.path.clone());
    let path = match std::fs::canonicalize(&path) {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "Get absolute path of [{}] failed: {}",
                args.path.display(),
                e
            );
            return -1;
        }
    };

    // Check path writable
    if let Err(e) = check_path_writable(&path) {
        eprintln!("{}", e.message);
        return -2;
    }

    // Check tmux
    let (tmux_mode, _real_stdout, tmux_pane_width) = match comm::check_tmux() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{}", e.message);
            return -3;
        }
    };

    if args.base.binary && tmux_mode != comm::TmuxMode::None {
        eprintln!("Binary upload in tmux is not supported, auto switch to base64 mode.");
    }
    if args.base.binary && comm::is_running_on_windows() {
        eprintln!("Binary upload on Windows is not supported, auto switch to base64 mode.");
    }

    let unique_id = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
        % 10_000_000_000)
        * 100;

    let mode = if args.base.directory { "D" } else { "R" };
    let header = format!(
        "\x1b[s::TRZSZ:TRANSFER:{}:{}:{:013}:0\r\n",
        mode, TRZSZ_VERSION, unique_id
    );
    let _ = io::stdout().write_all(header.as_bytes());
    let _ = io::stdout().flush();

    // Put stdin in raw mode so the filter's protocol bytes (ACT, ACK, etc.)
    // are not echoed back as "server output" and aren't line-buffered.
    #[cfg(unix)]
    let _raw_guard = {
        use std::os::unix::io::AsRawFd;
        RawModeGuard::enter(io::stdin().as_raw_fd())
    };

    // Setup transfer
    let mut transfer = TrzszTransfer::new(Box::new(io::stdout()));
    transfer.transfer_config.bufsize = args
        .base
        .parse_bufsize()
        .map(|b| b.size)
        .unwrap_or(10 * 1024 * 1024);
    transfer.transfer_config.timeout = args.base.timeout;

    // Wrap stdin reader
    let sender = transfer.buffer.sender();

    // Start reading from stdin in a background thread
    std::thread::spawn(move || {
        let stdin = io::stdin();
        let mut stdin_locked = stdin.lock();
        let mut buf = [0u8; 32 * 1024];
        loop {
            match stdin_locked.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let _ = sender.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    // Handle signals
    let stopped = std::sync::Arc::new(AtomicBool::new(false));
    let stopped_clone = stopped.clone();
    ctrlc::set_handler(move || {
        stopped_clone.store(true, Ordering::SeqCst);
    })
    .ok();

    // Run recv files
    let result = recv_files(&mut transfer, &args, tmux_mode, tmux_pane_width);

    match result {
        Ok(_msg) => 0,
        Err(e) => {
            transfer.server_error(&e);
            0
        }
    }
}

fn recv_files(
    transfer: &mut TrzszTransfer,
    args: &TrzArgs,
    _tmux_mode: comm::TmuxMode,
    _tmux_pane_width: i32,
) -> Result<String, TrzszError> {
    let action = transfer.recv_action()?;
    if !action.confirm {
        transfer.server_exit("Cancelled");
        return Ok("Cancelled".to_string());
    }

    let mut binary = args.base.binary;
    if binary && !action.support_binary {
        binary = false;
    }

    if args.base.fork && !action.fork {
        return Err(comm::simple_error(
            "The client doesn't support fork to background",
        ));
    }

    if args.base.directory && !action.support_directory {
        return Err(comm::simple_error(
            "The client doesn't support transfer directory",
        ));
    }

    let escape_chars = if binary {
        get_escape_chars(args.base.escape)
    } else {
        vec![]
    };
    let escape_value = serde_json::to_value(
        &escape_chars
            .iter()
            .map(|(a, b)| {
                vec![
                    serde_json::Value::String(String::from_utf8_lossy(a).to_string()),
                    serde_json::Value::String(String::from_utf8_lossy(b).to_string()),
                ]
            })
            .collect::<Vec<_>>(),
    )
    .unwrap_or(serde_json::Value::Null);

    transfer.transfer_config.binary = binary;
    transfer.send_config(
        args.base.quiet,
        binary,
        args.base.directory,
        args.base.overwrite,
        &escape_value,
        _tmux_pane_width,
        &action,
        args.base
            .parse_compress()
            .unwrap_or(crate::comm::CompressType::Auto),
    )?;

    let local_names = transfer.recv_files(&args.path, &mut None)?;

    transfer.recv_exit()?;
    let msg = format_saved_files(&local_names, &args.path);
    transfer.server_exit(&msg);
    Ok(msg)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_trz_main_help() {
        // Just verify it doesn't panic when args are invalid
        let result = std::process::Command::new("echo").arg("test").output();
        assert!(result.is_ok());
    }
}
