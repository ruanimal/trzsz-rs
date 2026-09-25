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

use crate::args::TrzszArgs;
use crate::comm::{self, TrzszError};
use crate::filter::{TrzszFilter, TrzszOptions};
use crate::version::TRZSZ_VERSION;

pub fn trzsz_main(args: &TrzszArgs) -> i32 {
    if args.version {
        println!("trzsz rust {}", TRZSZ_VERSION);
        return 0;
    }

    if args.args.is_empty() {
        // clap already prints help via --help, but if no args given, print usage
        eprintln!("usage: trzsz [-h] [-v] [-r] [-t] [-d] [-z] [-o] command line");
        return 0;
    }

    // Register cleanup on exit (restore terminal state)
    let _ = ctrlc::set_handler(|| {
        // Best-effort terminal restore on Ctrl+C
        use std::io::Write;
        let mut out = std::io::stdout();
        let _ = out.write_all(b"\x1b[?25h"); // show cursor
        let _ = out.write_all(b"\x1b[0m"); // reset attributes
        let _ = out.flush();
    });

    // Setup virtual terminal on Windows
    if let Err(e) = setup_virtual_terminal() {
        eprintln!("setup virtual terminal failed: {}\r\n", e);
        return -1;
    }

    // Spawn a pty
    #[cfg(feature = "pty")]
    let (pty_stdin, pty_stdout, mut child) = match spawn_pty(&args.args) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("spawn pty failed: {}\r\n", e);
            return -1;
        }
    };
    #[cfg(not(feature = "pty"))]
    let (pty_stdin, pty_stdout, mut child) = match spawn_pty_fallback(&args.args) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("spawn pty failed: {}\r\n", e);
            return -1;
        }
    };

    if args.relay {
        // Run as relay
        let options = TrzszOptions {
            detect_trace_log: args.tracelog,
            ..Default::default()
        };
        let mut filter = TrzszFilter::new(
            Box::new(io::stdin()),
            Box::new(io::stdout()),
            pty_stdin,
            pty_stdout,
            options,
        );
        filter.read_trzsz_config();

        // Handle signals
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped_clone = stopped.clone();
        ctrlc::set_handler(move || {
            stopped_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .ok();
    } else {
        // New trzsz filter
        let columns = get_terminal_columns();
        let options = TrzszOptions {
            terminal_columns: columns,
            detect_drag_file: args.dragfile,
            detect_trace_log: args.tracelog,
            enable_zmodem: args.zmodem,
            enable_osc52: args.osc52,
        };
        let mut filter = TrzszFilter::new(
            Box::new(io::stdin()),
            Box::new(io::stdout()),
            pty_stdin,
            pty_stdout,
            options,
        );
        filter.read_trzsz_config();

        // Handle signals
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped_clone = stopped.clone();
        ctrlc::set_handler(move || {
            stopped_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .ok();
    }

    // Wait for child process
    let status = child.wait().unwrap_or_default();
    status.code().unwrap_or(-1)
}

fn setup_virtual_terminal() -> Result<(), TrzszError> {
    #[cfg(windows)]
    {
        // Windows virtual terminal setup
    }
    Ok(())
}

fn get_terminal_columns() -> i32 {
    comm::get_terminal_columns()
}

/// Spawn a pseudo-terminal and run the command.
#[cfg(feature = "pty")]
fn spawn_pty(
    args: &[String],
) -> Result<
    (
        Box<dyn Write + Send>,
        Box<dyn Read + Send>,
        Box<dyn Child + Send>,
    ),
    TrzszError,
> {
    let cmd = &args[0];
    let cmd_args = &args[1..];

    use portable_pty::{CommandBuilder, PtySize, native_pty_system};
    let pty_system = native_pty_system();

    let pty_size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };
    let pty_pair = pty_system
        .openpty(pty_size)
        .map_err(|e| comm::simple_trzsz_error("Open PTY failed", e))?;

    let mut cmd_builder = CommandBuilder::new(cmd);
    cmd_builder.args(cmd_args);

    let child = pty_pair
        .slave
        .spawn_command(cmd_builder)
        .map_err(|e| comm::simple_trzsz_error("Spawn command on PTY failed", e))?;

    // The master side is the pty handle for reading/writing
    let reader = pty_pair
        .master
        .try_clone_reader()
        .map_err(|e| comm::simple_trzsz_error("Clone PTY reader failed", e))?;
    let writer = pty_pair
        .master
        .take_writer()
        .map_err(|e| comm::simple_trzsz_error("Take PTY writer failed", e))?;

    Ok((
        Box::new(writer),
        Box::new(reader),
        Box::new(PtyChild {
            child,
            _pty_pair: pty_pair,
        }),
    ))
}

/// Fallback when pty feature is disabled (no portable_pty support on this target).
#[cfg(not(feature = "pty"))]
fn spawn_pty_fallback(
    args: &[String],
) -> Result<
    (
        Box<dyn Write + Send>,
        Box<dyn Read + Send>,
        Box<dyn Child + Send>,
    ),
    TrzszError,
> {
    use std::process::Command;

    let cmd = &args[0];
    let cmd_args = &args[1..];

    let mut child = Command::new(cmd)
        .args(cmd_args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| comm::simple_trzsz_error("Spawn command failed", e))?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| comm::simple_trzsz_error("Take stdin failed", "No stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| comm::simple_trzsz_error("Take stdout failed", "No stdout"))?;

    Ok((Box::new(stdin), Box::new(stdout), Box::new(child)))
}

#[cfg(feature = "pty")]
struct PtyChild {
    child: Box<dyn portable_pty::Child + Send>,
    _pty_pair: portable_pty::PtyPair,
}

#[cfg(feature = "pty")]
impl Child for PtyChild {
    fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        let status = self
            .child
            .wait()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // portable_pty ExitStatus wraps the process ExitStatus
        // Convert via exit_code() -> from_raw
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            Ok(std::process::ExitStatus::from_raw(status.exit_code() as i32))
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::ExitStatusExt;
            Ok(std::process::ExitStatus::from_raw(status.exit_code() as u32))
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = status;
            Ok(std::process::ExitStatus::from_raw(0))
        }
    }
}

trait Child {
    fn wait(&mut self) -> io::Result<std::process::ExitStatus>;
}

#[cfg(not(feature = "pty"))]
impl Child for std::process::Child {
    fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        std::process::Child::wait(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_trzsz_help() {
        let args = TrzszArgs::try_parse_from(["trzsz"]).unwrap();
        assert_eq!(trzsz_main(&args), 0);
    }

    #[test]
    fn test_trzsz_version() {
        let args = TrzszArgs::try_parse_from(["trzsz", "-v"]).unwrap();
        assert_eq!(trzsz_main(&args), 0);
    }

    #[test]
    fn test_trzsz_no_args() {
        let args = TrzszArgs::try_parse_from(["trzsz"]).unwrap();
        assert_eq!(trzsz_main(&args), 0);
    }
}
