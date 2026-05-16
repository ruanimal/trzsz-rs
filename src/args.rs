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

use clap::Parser;
use std::path::PathBuf;

use crate::comm::{BufferSize, CompressType};

// ─── Base args (shared between trz and tsz) ────────────────────────────────

#[derive(Debug, Clone, Parser)]
#[command(author, version, about)]
pub struct BaseArgs {
    /// quiet (hide progress bar)
    #[arg(short = 'q', long = "quiet")]
    pub quiet: bool,

    /// yes, overwrite existing file(s)
    #[arg(short = 'y', long = "overwrite")]
    pub overwrite: bool,

    /// binary transfer mode, faster for binary files
    #[arg(short = 'b', long = "binary")]
    pub binary: bool,

    /// escape all known control characters
    #[arg(short = 'e', long = "escape")]
    pub escape: bool,

    /// transfer directories and files
    #[arg(short = 'd', long = "directory")]
    pub directory: bool,

    /// transfer directories and files, same as -d
    #[arg(short = 'r', long = "recursive")]
    pub recursive: bool,

    /// fork to transfer in background (implies -q)
    #[arg(short = 'f', long = "fork")]
    pub fork: bool,

    /// max buffer chunk size (1K<=N<=1G)
    #[arg(short = 'B', long = "bufsize", default_value = "10M")]
    pub bufsize: String,

    /// timeout (N seconds) for each buffer chunk. N <= 0 means never timeout
    #[arg(short = 't', long = "timeout", default_value = "20")]
    pub timeout: i32,

    /// compress type
    #[arg(short = 'c', long = "compress", default_value = "auto")]
    pub compress: String,
}

impl BaseArgs {
    pub fn parse_bufsize(&self) -> Result<BufferSize, String> {
        BufferSize::parse(&self.bufsize).map_err(|e| e.message)
    }

    pub fn parse_compress(&self) -> Result<CompressType, String> {
        CompressType::from_str(&self.compress).map_err(|e| e.message)
    }

    /// Returns effective directory flag (recursive implies directory).
    pub fn effective_directory(&self) -> bool {
        self.directory || self.recursive
    }
}

// ─── trz args ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Parser)]
#[command(
    name = "trz",
    about = "Receive file(s), similar to rz and compatible with tmux."
)]
pub struct TrzArgs {
    #[command(flatten)]
    pub base: BaseArgs,

    /// path to save file(s)
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

// ─── tsz args ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Parser)]
#[command(
    name = "tsz",
    about = "Send file(s), similar to sz and compatible with tmux."
)]
pub struct TszArgs {
    #[command(flatten)]
    pub base: BaseArgs,

    /// file(s) to be sent
    #[arg(required = true)]
    pub file: Vec<PathBuf>,
}

// ─── trzsz args ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Parser)]
#[command(
    name = "trzsz",
    about = "Wrapping command line to support trzsz (trz / tsz)."
)]
pub struct TrzszArgs {
    /// show version number and exit
    #[arg(short = 'v', long = "version")]
    pub version: bool,

    /// run as a trzsz relay server
    #[arg(short = 'r', long = "relay")]
    pub relay: bool,

    /// enable trace log for debugging
    #[arg(short = 't', long = "tracelog")]
    pub tracelog: bool,

    /// enable drag file(s) to upload
    #[arg(short = 'd', long = "dragfile")]
    pub dragfile: bool,

    /// enable zmodem lrzsz (rz / sz)
    #[arg(short = 'z', long = "zmodem")]
    pub zmodem: bool,

    /// enable clipboard integration
    #[arg(short = 'o', long = "osc52")]
    pub osc52: bool,

    /// the original command line
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trz_args_defaults() {
        let args = TrzArgs::try_parse_from(["trz"]).unwrap();
        assert!(!args.base.quiet);
        assert!(!args.base.overwrite);
        assert!(!args.base.binary);
        assert!(!args.base.escape);
        assert!(!args.base.directory);
        assert!(!args.base.recursive);
        assert!(!args.base.fork);
        assert_eq!(args.base.bufsize, "10M");
        assert_eq!(args.base.timeout, 20);
        assert_eq!(args.base.compress, "auto");
        assert_eq!(args.path, PathBuf::from("."));
    }

    #[test]
    fn test_trz_args_short_flags() {
        let args = TrzArgs::try_parse_from(["trz", "-q"]).unwrap();
        assert!(args.base.quiet);

        let args = TrzArgs::try_parse_from(["trz", "-y"]).unwrap();
        assert!(args.base.overwrite);

        let args = TrzArgs::try_parse_from(["trz", "-b"]).unwrap();
        assert!(args.base.binary);

        let args = TrzArgs::try_parse_from(["trz", "-e"]).unwrap();
        assert!(args.base.escape);

        let args = TrzArgs::try_parse_from(["trz", "-d"]).unwrap();
        assert!(args.base.directory);

        let args = TrzArgs::try_parse_from(["trz", "-r"]).unwrap();
        assert!(args.base.recursive);
        assert!(args.base.effective_directory()); // -r implies -d

        let args = TrzArgs::try_parse_from(["trz", "-f"]).unwrap();
        assert!(args.base.fork);
    }

    #[test]
    fn test_trz_args_long_flags() {
        let args = TrzArgs::try_parse_from(["trz", "--quiet"]).unwrap();
        assert!(args.base.quiet);

        let args = TrzArgs::try_parse_from(["trz", "--overwrite"]).unwrap();
        assert!(args.base.overwrite);

        let args = TrzArgs::try_parse_from(["trz", "--binary"]).unwrap();
        assert!(args.base.binary);

        let args = TrzArgs::try_parse_from(["trz", "--escape"]).unwrap();
        assert!(args.base.escape);

        let args = TrzArgs::try_parse_from(["trz", "--directory"]).unwrap();
        assert!(args.base.directory);

        let args = TrzArgs::try_parse_from(["trz", "--recursive"]).unwrap();
        assert!(args.base.recursive);

        let args = TrzArgs::try_parse_from(["trz", "--fork"]).unwrap();
        assert!(args.base.fork);
    }

    #[test]
    fn test_trz_args_bufsize() {
        let args = TrzArgs::try_parse_from(["trz", "-B", "2k"]).unwrap();
        assert_eq!(args.base.bufsize, "2k");

        let args = TrzArgs::try_parse_from(["trz", "-B1024"]).unwrap();
        assert_eq!(args.base.bufsize, "1024");

        let args = TrzArgs::try_parse_from(["trz", "--bufsize", "2M"]).unwrap();
        assert_eq!(args.base.bufsize, "2M");

        let args = TrzArgs::try_parse_from(["trz", "-B", "1MB"]).unwrap();
        assert_eq!(args.base.bufsize, "1MB");
    }

    #[test]
    fn test_trz_args_compress() {
        let args = TrzArgs::try_parse_from(["trz", "-c", "No"]).unwrap();
        assert_eq!(args.base.compress, "No");

        let args = TrzArgs::try_parse_from(["trz", "-c", "yes"]).unwrap();
        assert_eq!(args.base.compress, "yes");

        let args = TrzArgs::try_parse_from(["trz", "-c", "AUTO"]).unwrap();
        assert_eq!(args.base.compress, "AUTO");

        let args = TrzArgs::try_parse_from(["trz", "--compress", "yes"]).unwrap();
        assert_eq!(args.base.compress, "yes");
    }

    #[test]
    fn test_trz_args_combined_flags() {
        let args = TrzArgs::try_parse_from(["trz", "-yq"]).unwrap();
        assert!(args.base.quiet);
        assert!(args.base.overwrite);

        let args = TrzArgs::try_parse_from(["trz", "-bed"]).unwrap();
        assert!(args.base.binary);
        assert!(args.base.escape);
        assert!(args.base.directory);
    }

    #[test]
    fn test_trz_args_positional() {
        let args = TrzArgs::try_parse_from(["trz", "/tmp"]).unwrap();
        assert_eq!(args.path, PathBuf::from("/tmp"));

        let args = TrzArgs::try_parse_from(["trz", "-y", "-d", "../adir"]).unwrap();
        assert!(args.base.overwrite);
        assert!(args.base.directory);
        assert_eq!(args.path, PathBuf::from("../adir"));
    }

    #[test]
    fn test_trz_args_timeout() {
        let args = TrzArgs::try_parse_from(["trz", "-t", "3"]).unwrap();
        assert_eq!(args.base.timeout, 3);

        let args = TrzArgs::try_parse_from(["trz", "--timeout", "55"]).unwrap();
        assert_eq!(args.base.timeout, 55);
    }

    #[test]
    fn test_tsz_args_defaults() {
        let args = TszArgs::try_parse_from(["tsz", "a"]).unwrap();
        assert!(!args.base.quiet);
        assert!(!args.base.overwrite);
        assert!(!args.base.binary);
        assert!(!args.base.escape);
        assert!(!args.base.directory);
        assert!(!args.base.recursive);
        assert!(!args.base.fork);
        assert_eq!(args.base.bufsize, "10M");
        assert_eq!(args.base.timeout, 20);
        assert_eq!(args.base.compress, "auto");
        assert_eq!(args.file, vec![PathBuf::from("a")]);
    }

    #[test]
    fn test_tsz_args_short_flags() {
        let args = TszArgs::try_parse_from(["tsz", "-q", "a"]).unwrap();
        assert!(args.base.quiet);

        let args = TszArgs::try_parse_from(["tsz", "-y", "a"]).unwrap();
        assert!(args.base.overwrite);

        let args = TszArgs::try_parse_from(["tsz", "-b", "a"]).unwrap();
        assert!(args.base.binary);

        let args = TszArgs::try_parse_from(["tsz", "-e", "a"]).unwrap();
        assert!(args.base.escape);

        let args = TszArgs::try_parse_from(["tsz", "-d", "a"]).unwrap();
        assert!(args.base.directory);

        let args = TszArgs::try_parse_from(["tsz", "-r", "a"]).unwrap();
        assert!(args.base.recursive);
        assert!(args.base.effective_directory());
    }

    #[test]
    fn test_tsz_args_long_flags() {
        let args = TszArgs::try_parse_from(["tsz", "--quiet", "a"]).unwrap();
        assert!(args.base.quiet);

        let args = TszArgs::try_parse_from(["tsz", "--overwrite", "a"]).unwrap();
        assert!(args.base.overwrite);

        let args = TszArgs::try_parse_from(["tsz", "--binary", "a"]).unwrap();
        assert!(args.base.binary);

        let args = TszArgs::try_parse_from(["tsz", "--escape", "a"]).unwrap();
        assert!(args.base.escape);

        let args = TszArgs::try_parse_from(["tsz", "--directory", "a"]).unwrap();
        assert!(args.base.directory);

        let args = TszArgs::try_parse_from(["tsz", "--recursive", "a"]).unwrap();
        assert!(args.base.recursive);

        let args = TszArgs::try_parse_from(["tsz", "--fork", "a"]).unwrap();
        assert!(args.base.fork);
    }

    #[test]
    fn test_tsz_args_bufsize() {
        let args = TszArgs::try_parse_from(["tsz", "-B", "2k", "a"]).unwrap();
        assert_eq!(args.base.bufsize, "2k");

        let args = TszArgs::try_parse_from(["tsz", "-B1024", "a"]).unwrap();
        assert_eq!(args.base.bufsize, "1024");

        let args = TszArgs::try_parse_from(["tsz", "--bufsize", "2M", "a"]).unwrap();
        assert_eq!(args.base.bufsize, "2M");
    }

    #[test]
    fn test_tsz_args_compress() {
        let args = TszArgs::try_parse_from(["tsz", "-cno", "a"]).unwrap();
        assert_eq!(args.base.compress, "no");

        let args = TszArgs::try_parse_from(["tsz", "-c", "Yes", "a"]).unwrap();
        assert_eq!(args.base.compress, "Yes");

        let args = TszArgs::try_parse_from(["tsz", "-c", "auto", "a"]).unwrap();
        assert_eq!(args.base.compress, "auto");
    }

    #[test]
    fn test_tsz_args_combined_flags() {
        let args = TszArgs::try_parse_from(["tsz", "-yq", "a"]).unwrap();
        assert!(args.base.quiet);
        assert!(args.base.overwrite);

        let args = TszArgs::try_parse_from(["tsz", "-bed", "a"]).unwrap();
        assert!(args.base.binary);
        assert!(args.base.escape);
        assert!(args.base.directory);
    }

    #[test]
    fn test_tsz_args_positional() {
        let args = TszArgs::try_parse_from(["tsz", "-y", "-d", "a", "b", "c"]).unwrap();
        assert!(args.base.overwrite);
        assert!(args.base.directory);
        assert_eq!(args.file, vec![PathBuf::from("a"), PathBuf::from("b"), PathBuf::from("c")]);
    }

    #[test]
    fn test_tsz_args_timeout() {
        let args = TszArgs::try_parse_from(["tsz", "-t", "3", "a"]).unwrap();
        assert_eq!(args.base.timeout, 3);
    }

    #[test]
    fn test_tsz_args_error_no_file() {
        let result = TszArgs::try_parse_from(["tsz"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_trzsz_args() {
        let args = TrzszArgs::try_parse_from(["trzsz", "-r", "ssh", "x.x.x.x"]).unwrap();
        assert!(args.relay);
        assert_eq!(args.args, vec!["ssh", "x.x.x.x"]);

        let args = TrzszArgs::try_parse_from(["trzsz", "-d", "ssh", "x.x.x.x"]).unwrap();
        assert!(args.dragfile);

        let args = TrzszArgs::try_parse_from(["trzsz", "-z", "ssh", "x.x.x.x"]).unwrap();
        assert!(args.zmodem);

        let args = TrzszArgs::try_parse_from(["trzsz", "-t", "ssh", "x.x.x.x"]).unwrap();
        assert!(args.tracelog);

        let args = TrzszArgs::try_parse_from(["trzsz", "-o", "ssh", "x.x.x.x"]).unwrap();
        assert!(args.osc52);
    }
}
