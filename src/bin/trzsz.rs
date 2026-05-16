/*
MIT License

Copyright (c) 2022-2026 The Trzsz Authors.
*/

use clap::Parser;
use trzsz_rs::args::TrzszArgs;

fn main() {
    let args = TrzszArgs::try_parse().unwrap_or_else(|e| {
        e.exit();
    });

    let rc = trzsz_rs::trzsz::trzsz_main(&args);
    std::process::exit(rc);
}
