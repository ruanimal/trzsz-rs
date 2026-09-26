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

use std::io::{self, Write};

use crate::args::TszArgs;
use crate::comm::{self, TrzszError, check_duplicate_names, check_paths_readable};
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

pub fn tsz_main(args: &TszArgs) -> i32 {
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

    // Check files readable
    let files = match check_paths_readable(&args.file, args.base.effective_directory()) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{}", e.message);
            return -1;
        }
    };

    if args.base.overwrite {
        if let Err(e) = check_duplicate_names(&files) {
            eprintln!("{}", e.message);
            return -2;
        }
    }

    // Check tmux
    let (tmux_mode, _real_stdout, tmux_pane_width) = match comm::check_tmux() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{}", e.message);
            return -3;
        }
    };

    if args.base.binary && tmux_mode == comm::TmuxMode::Control {
        eprintln!("Binary download in tmux control mode is slower, auto switch to base64 mode.");
    }
    if args.base.binary && comm::is_running_on_windows() {
        eprintln!("Binary download on Windows is not supported, auto switch to base64 mode.");
    }

    let (tunnel_listener, tunnel_port) = comm::listen_for_tunnel();
    let unique_id = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
        % 10_000_000_000)
        * 100;

    let header = format!(
        "\x1b[s::TRZSZ:TRANSFER:S:{}:{:013}:{}\r\n",
        TRZSZ_VERSION, unique_id, tunnel_port
    );
    let _ = io::stdout().write_all(header.as_bytes());
    let _ = io::stdout().flush();

    // Put stdin in raw mode so the filter's protocol bytes (ACT, ACK, etc.)
    // are not echoed back as "server output", aren't line-buffered, and
    // don't get translated by the terminal line discipline. Matches the
    // behavior of trzsz-go's tsz which calls term.MakeRaw on startup.
    #[cfg(unix)]
    let _raw_guard = {
        use std::os::unix::io::AsRawFd;
        RawModeGuard::enter(io::stdin().as_raw_fd())
    };

    // Setup transfer
    let mut transfer = TrzszTransfer::new(Box::new(io::stdout()));
    if let Some(listener) = tunnel_listener {
        transfer.accept_on_tunnel(listener, format!("{:013}", unique_id), tunnel_port);
    }
    transfer.transfer_config.bufsize = args
        .base
        .parse_bufsize()
        .map(|b| b.size)
        .unwrap_or(10 * 1024 * 1024);
    transfer.transfer_config.timeout = args.base.timeout;

    let sender = transfer.buffer.sender();
    let interrupt_controller = crate::stop_prompt::StopPromptController::new(&transfer);
    let input_controller = interrupt_controller.clone();
    std::thread::spawn(move || {
        let stdin = io::stdin();
        crate::stop_prompt::run_stdin_reader(stdin.lock(), sender, input_controller);
    });
    let signal_controller = interrupt_controller;
    ctrlc::set_handler(move || signal_controller.handle_interrupt()).ok();

    let result = send_files(&mut transfer, &files, &args, tmux_mode, tmux_pane_width);

    match result {
        Ok(_msg) => 0,
        Err(e) => {
            transfer.server_error(&e);
            0
        }
    }
}

fn send_files(
    transfer: &mut TrzszTransfer,
    files: &[crate::comm::SourceFile],
    args: &TszArgs,
    tmux_mode: comm::TmuxMode,
    tmux_pane_width: i32,
) -> Result<String, TrzszError> {
    let action = transfer.recv_action()?;
    if !action.confirm {
        transfer.server_exit("Cancelled");
        return Ok("Cancelled".to_string());
    }

    let binary = comm::binary_mode_enabled(
        args.base.binary,
        action.support_binary,
        tmux_mode != comm::TmuxMode::Control,
        comm::is_running_on_windows(),
    );

    if args.base.fork && !action.fork {
        return Err(comm::simple_error(
            "The client doesn't support fork to background",
        ));
    }

    let directory = args.base.effective_directory();
    if directory && !action.support_directory {
        return Err(comm::simple_error(
            "The client doesn't support transfer directory",
        ));
    }

    let escape_value = serde_json::Value::Null;

    transfer.transfer_config.binary = binary;
    transfer.transfer_config.fork = args.base.fork;
    transfer.transfer_config.tmux_output_junk = tmux_mode == comm::TmuxMode::Normal;
    transfer.send_config(
        args.base.quiet || args.base.fork,
        binary,
        directory,
        args.base.overwrite,
        &escape_value,
        tmux_pane_width,
        &action,
        args.base
            .parse_compress()
            .unwrap_or(crate::comm::CompressType::Auto),
    )?;

    // The client filter renders transfer progress; match trzsz-go's CLI behavior.
    let _remote_names = transfer.send_files(files, &mut None)?;

    let msg = transfer.recv_exit()?;
    transfer.server_exit(&msg);
    Ok(msg)
}

#[cfg(test)]
mod tests {}
