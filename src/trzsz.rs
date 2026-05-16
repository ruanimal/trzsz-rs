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
use std::path::Path;

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
        print_trzsz_help();
        return 0;
    }

    // Cleanup on exit would be registered here

    // Setup virtual terminal on Windows
    if let Err(e) = setup_virtual_terminal() {
        eprintln!("setup virtual terminal failed: {}\r\n", e);
        return -1;
    }

    // Spawn a pty
    let (pty_stdin, pty_stdout, mut child) = match spawn_pty(&args.args) {
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
        let filter = TrzszFilter::new(
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
        }).ok();
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
        let filter = TrzszFilter::new(
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
        }).ok();
    }

    // Wait for child process
    let status = child.wait().unwrap_or_default();
    status.code().unwrap_or(-1)
}

fn print_trzsz_help() {
    eprint!("usage: trzsz [-h] [-v] [-r] [-t] [-d] [-z] [-o] command line\n\n");
    eprint!("Wrapping command line to support trzsz ( trz / tsz ).\n\n");
    eprint!("positional arguments:\n");
    eprint!("  command line       the original command line\n\n");
    eprint!("optional arguments:\n");
    eprint!("  -h, --help         show this help message and exit\n");
    eprint!("  -v, --version      show version number and exit\n");
    eprint!("  -r, --relay        run as a trzsz relay server\n");
    eprint!("  -t, --tracelog     enable trace log for debugging\n");
    eprint!("  -d, --dragfile     enable drag file(s) to upload\n");
    eprint!("  -z, --zmodem       enable zmodem lrzsz (rz / sz)\n");
    eprint!("  -o, --osc52        enable clipboard integration\n");
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
fn spawn_pty(args: &[String]) -> Result<(Box<dyn Write + Send>, Box<dyn Read + Send>, Box<dyn Child>), TrzszError> {
    let cmd = &args[0];
    let cmd_args = &args[1..];

    #[cfg(unix)]
    {
        use std::process::Command;
        let mut cmd = Command::new(cmd);
        cmd.args(cmd_args);
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| {
            comm::simple_trzsz_error("Spawn process failed", e)
        })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        Ok((Box::new(stdin), Box::new(stdout), Box::new(child)))
    }
    #[cfg(not(unix))]
    {
        use std::process::Command;
        let mut cmd = Command::new(cmd);
        cmd.args(cmd_args);
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| {
            comm::simple_trzsz_error("Spawn process failed", e)
        })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        Ok((Box::new(stdin), Box::new(stdout), Box::new(child)))
    }
}

trait Child {
    fn wait(&mut self) -> io::Result<std::process::ExitStatus>;
}

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
