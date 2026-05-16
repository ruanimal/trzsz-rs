## Plan: Port trzsz-go CLI to Rust

TL;DR - Build the Rust CLI implementation for `trzsz`, `trz`, and `tsz` based on the Go submodule. Create a Rust crate with shared internal modules for argument parsing, pty handling, transfer logic, and the `trzsz` wrapper filter. Add unit tests for parser behavior, path validation, and command entrypoints.

> **Core Principle**: Use existing, well-maintained Rust crates for all standard functionality. Only implement custom logic where no suitable crate exists.

**Recommended Crate Mapping (Go → Rust)**

| 功能 | Go 库 | Rust 替代库 |
|------|-------|-------------|
| 命令行解析 | `trzsz/go-arg` | **`clap`** (derive 模式) |
| PTY 伪终端 | `creack/pty` | **`portable-pty`** |
| 终端 raw mode | `golang.org/x/term` | **`rustyline`** 或直接 `termios` via `libc` |
| 信号处理 | `os/signal` | **`signal-hook`** + `signal-hook-tokio` |
| 数据压缩 | `klauspost/compress` (zstd) | **`zstd-stream`** 或 `libz-sys` (gzip) |
| Shell 分词 | `google/shlex` | **`shell-words`** |
| 剪贴板 | `atotto/clipboard` | **`arboard`** |
| 文件选择对话框 | `ncruces/zenity` | **`rfd`** (原生文件对话框) |
| 进度条显示 | 自定义 (lipgloss) | **`indicatif`** |
| VT100/ANSI 解析 | `charmbracelet/x/ansi` | **`vte`** + `vte_generate_state_machine` |
| 字符宽度 | `mattn/go-runewidth` | **`unicode-width`** |
| JSON 序列化 | `encoding/json` | **`serde`** + `serde_json` |
| 颜色渲染 | `lucasb-eyer/go-colorful` | **`owo-colors`** 或 `console` |
| 文件/路径操作 | `path/filepath` | 标准库 `std::path` / `std::fs` |
| IO 操作 | `io`, `bufio` | 标准库 `std::io` / `tokio::io` |
| 子进程管理 | `os/exec` | **`std::process::Command`** / `tokio::process::Command` |
| 网络 TCP | `net` | 标准库 `std::net` / `tokio::net` |
| 时区/终端检测 | `os`, `syscall` | **`sys-info`** / `std::env` |
| 测试框架 | `testify` | 标准库 `#[test]` + **`assert_cmd`** (CLI 测试) |
| 字符编码 (charmap) | `golang.org/x/text/encoding/charmap` | **`encoding_rs`** |

**Steps**
1. Define the Rust crate structure.
   - Convert the existing single `src/main.rs` into a library crate plus three binaries:
     - `src/lib.rs`
     - `src/bin/trzsz.rs`
     - `src/bin/trz.rs`
     - `src/bin/tsz.rs`
   - Update `Cargo.toml` with binary targets and dependencies.

2. Implement shared CLI argument parsing (using `clap` derive).
   - Create `src/args.rs` with `#[derive(Parser)]` structs.
   - Mirror the Go `baseArgs`, `trzArgs`, `tszArgs`, and `trzszArgs` semantics.
   - Use `clap`'s built-in support for value parsing to handle size suffixes (`-B 1M`, `-B1G`, etc.).
   - Use `clap`'s value_enum for compress option parsing.
   - Add unit tests for parser behavior using the Go test cases as a spec.

3. Implement terminal and pty support (using `portable-pty` + `signal-hook`).
   - Add `src/pty.rs` to spawn subprocesses under a pseudo-terminal and manage terminal raw mode.
   - Use `portable-pty` for cross-platform PTY creation.
   - Use `signal-hook` for SIGTERM/SIGWINCH handling.
   - Support terminal resize notifications and cleanup on exit.

4. Port the Go `trz` and `tsz` command workflows.
   - Add `src/trz.rs` and `src/tsz.rs` (internal modules) for transfer startup logic.
   - Use `shell-words` for drag file upload command parsing (replaces `google/shlex`).
   - Use `rfd` for file chooser dialogs (replaces `ncruces/zenity`).
   - Implement file path validation, fork-to-background behavior, tmux detection, and transfer tunnel listener setup from the Go code.
   - Port the `recvFiles` and `sendFiles` flows with equivalent error handling.

5. Implement the `trzsz` wrapper behavior.
   - Add `src/trzsz.rs` and internal filter logic needed for `trzsz` command operation.
   - Support command-line flags `-r`, `-t`, `-d`, `-z`, `-o`, and the relay mode.
   - Wire `trzsz` to spawn a pty for the target command and wrap I/O through the filter.
   - Implement `TrzszFilter` internals only as required by CLI behavior, not as a public library API.

6. Add cross-platform support points.
   - Use conditional modules for Windows/macOS vs. Unix terminal handling.
   - Use `#[cfg(target_os = "...")]` for platform-specific code.
   - Use `portable-pty` for PTY abstraction across platforms.
   - Use `crossterm` for terminal manipulation as a fallback where needed.

7. Add tests.
   - Create Rust unit tests for parser parsing and validation in `src/args.rs`.
   - Use `assert_cmd` for CLI integration tests (help output, version output, error messages).
   - Add tests for path checks and `trz`/`tsz` option normalization.
   - Add basic command entrypoint tests for `--help` and `--version` formatting.

**Relevant files**
- `Cargo.toml` — add binaries and dependencies.
- `src/main.rs` — replace with library entry or remove.
- `src/lib.rs` — core implementation modules.
- `src/bin/trzsz.rs` — CLI wrapper for the `trzsz` command.
- `src/bin/trz.rs` — CLI wrapper for the `trz` command.
- `src/bin/tsz.rs` — CLI wrapper for the `tsz` command.
- `src/args.rs` — argument parsing and flag handling.
- `src/pty.rs` — pty spawn and terminal handling.
- `src/trz.rs` — receive-side transfer logic.
- `src/tsz.rs` — send-side transfer logic.
- `src/trzsz.rs` — wrapper filter and I/O filter logic.

**Verification**
1. Run `cargo test` and ensure the parser tests pass.
2. Verify `cargo run --bin trz --help`, `cargo run --bin tsz --help`, and `cargo run --bin trzsz --help` work.
3. Validate binary startup on Linux and ensure compile passes for Windows/macOS targets with conditional code.
4. Confirm the core transfer command behavior by comparing argument parsing and command outputs to the Go test cases.

**Decisions**
- **尽量使用 Rust 生态中的成熟 crate**，非必要不自己实现。核心原则：命令行解析用 `clap`，PTY 用 `portable-pty`，终端操作用 `rustyline`/`crossterm`，压缩用 `zstd-stream`，进度条用 `indicatif`，文件对话框用 `rfd`，剪贴板用 `arboard`，ANSI 解析用 `vte`，Shell 分词用 `shell-words`，字符宽度用 `unicode-width`。
- Scope is limited to CLI commands (`trzsz`, `trz`, `tsz`) with internal filter support, not a public Rust library API.
- The Rust port will use an idiomatic crate layout with shared internal modules and multiple binaries.
- Cross-platform support is required, so implementation must include platform-specific terminal handling.

**Further Considerations**
1. If you want a faster initial delivery, I can start with Unix-only support and add Windows/macOS support in a follow-up.
2. The full Go package includes complex zmodem and tmux behavior; the Rust plan assumes we will port the CLI workflows and the required internal filter logic rather than the entire Go `trzsz` API surface.
3. Crate selection should prioritize crates that are actively maintained, have good documentation, and support the `tokio` async ecosystem if async is needed later.
