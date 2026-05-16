# File transfer bug hunt (2026-05)

## Symptom

Running `tsz <file>` (or `trz <dir>`) inside a `trzsz-go` filter would
either hang forever during data transfer, or finish the transfer but
leave the progress bar on screen and never return to the shell prompt.

## Investigation method

The bug was reproduced and bisected with three layered tests:

1. **In-memory end-to-end** (`tests/transfer_e2e.rs`): two
   `TrzszTransfer` instances cross-wired through `mpsc` channels.
   Verified the protocol is correct in isolation. Caught the buffer
   data-loss bug.
2. **Synthetic protocol** (`tests/protocol_minimal.rs`): drives the
   real `tsz` process through `stdin`/`stdout`, simulating the Go
   filter's side of the protocol byte-for-byte. Pinpoints exactly
   which protocol step a real `tsz` binary fails on.
3. **Real interop** (`tests/interop.rs`): runs `trzsz-go`'s filter in a
   real PTY and lets it talk to `rs-tsz`. Reproduces the user's
   environment as closely as possible. Requires:
   - `/tmp/go-trzsz`, `/tmp/go-tsz` (built from `trzsz-go/cmd/...`)
   - `~/.trzsz.conf` containing `DefaultDownloadPath = /tmp/trzsz_dl`

A `baseline` interop test (Go filter ↔ Go tsz) confirms the harness
itself is correct so failures point unambiguously at `trzsz-rs`.

## Bugs found and fixed

Each entry lists the symptom, the root cause, and the fix.

### 1. Buffer loses bytes after the first read

**File:** `src/buffer.rs`

`TrzszBuffer::next_buffer` cleared `next_buf` after returning the
remaining slice. Callers (`read_line`, `read_binary`) then assigned
`next_idx = N` instead of `+= N`. The next call to `next_buffer` saw
`next_buf = None`, pulled a brand-new chunk from the channel, and the
unread bytes from the previous chunk were silently discarded.

In a real transfer, stdin reads up to 32 KiB at a time, so a single
chunk routinely contained `#DATA:N\n<N bytes><next header>`. The
header was parsed, then the binary payload that followed was lost.

**Fix:**
- `next_buffer` now returns the slice without clearing `next_buf`.
- `read_line`, `read_binary`, and `read_line_on_windows` advance
  `next_idx += N` so subsequent calls continue reading from the same
  buffer.
- Added regression tests for the three "data after newline in same
  chunk" patterns.

### 2. Empty `rel_path` makes the receiver write to a directory

**File:** `src/comm.rs`

`check_paths_readable` started recursion with `rel_path = vec![]`. For
single-file transfers this left `rel_path` empty, so
`SourceFile::get_file_name()` returned `""`. The Go filter then tried
to create a file named `""` inside its download directory, which it
saw as the directory itself and aborted with `Is a directory: ...`.

**Fix:** initial `rel_path` is now `vec![abs_path.file_name()]`,
matching `trzsz-go/comm.go: checkPathsReadable`.

### 3. Stdin in cooked mode echoes filter bytes back

**Files:** `src/tsz.rs`, `src/trz.rs`

The Rust binaries did not put `stdin` into raw mode. The PTY line
discipline echoed every byte the filter wrote (`#ACT:`, `#SUCC:`, …)
back as "server output". The filter then read its own bytes from
`serverOut`, mistook them for a server response, and the protocol
de-synced.

**Fix:** `RawModeGuard` (RAII) wraps `nix::sys::termios::cfmakeraw` /
`tcsetattr` and restores the saved termios on drop. Used by both
`tsz_main` and `trz_main`. Mirrors `term.MakeRaw` in `trzsz-go`.

### 4. Wrong digest algorithm

**Files:** `src/transfer.rs`, `Cargo.toml`

The protocol uses MD5 for the integrity check. `trzsz-rs` was
computing SHA-256 and sending it in the `MD5` line, so the receiver
always answered `Check MD5 failed`.

**Fix:** swap to the `md-5` crate; replace `Sha256::new()` with
`Md5::new()`.

### 5. Stdout buffering delays protocol bytes

**File:** `src/transfer.rs`

`TrzszTransfer::write_all` did not flush. When stdout was a pipe (any
non-TTY case the filter sees), bytes sat in the OS buffer and the
peer waited for chunks that had already been "sent".

**Fix:** flush `self.writer` at the end of every `write_all`.

### 6. Advertised protocol version > implemented

**File:** `src/transfer.rs`

`K_PROTOCOL_VERSION` was 4, but `trzsz-rs` only implements the V1
synchronous stop-and-wait scheme. V2+ uses different ack syntax
(`#SUCC:length/step`) and a streaming encode/decode pipeline. Peers
seeing protocol ≥ 2 would send acks `trzsz-rs` could not parse.

**Fix:** advertise `K_PROTOCOL_VERSION = 1` until V2+ is implemented.

### 7. Infinite loop in `clean_input`

**File:** `src/transfer.rs`

The original implementation captured `now` once outside the loop and
used `remaining = timeout - (now - last)`. `last` is only updated by
`add_received_data`, which `tsz`/`trz` never call (their stdin reader
sends straight to the buffer). Result: `last == now` forever, so
`remaining == timeout` on every iteration and the loop slept the full
timeout repeatedly. `server_exit` therefore never returned and the
process never exited even after the transfer succeeded.

**Fix:** recompute `now_ms` each iteration, matching Go's
`time.Since(time.UnixMilli(t.lastInputTime.Load()))`.

### 8. `reset_term` branches were inverted

**File:** `src/transfer.rs`

The `ignorable` parameter was being read backward:
- `ignorable=false` (the success path) drew the green banner across the
  top of the screen instead of clearing the progress bar.
- `ignorable=true` did the cursor-restore-and-clear that the success
  path needed.

After a transfer the user saw the progress bar still on screen with
the success message painted on top.

**Fix:** mirror `trzsz-go/transfer.go: resetTerm`:
- Add a `term_reseted: AtomicBool` flag.
- First call (CAS succeeds): issue `\x1b[u\x1b[0J`, write the message,
  show the cursor, flush. This wipes the progress bar.
- Subsequent calls only paint the green banner when `ignorable=false`.
- Termios is restored separately by `RawModeGuard::drop`.

## Test status after all fixes

```
unit tests              45 passed
transfer_e2e             3 passed   (rs ↔ rs in-memory)
protocol_minimal         1 passed   (rs-tsz vs hand-rolled protocol)
interop_baseline         1 passed   (go-trzsz ↔ go-tsz)
interop                  3 passed   (go-trzsz ↔ rs-tsz: small, large, exit)
```

`test_rs_tsz_exits_after_transfer` was specifically verified to FAIL
on the pre-fix `clean_input` (3 s deadline missed) and PASS after.

## Files changed

```
Cargo.lock      | 11 +++
Cargo.toml      |  1 +    (md-5 crate)
src/buffer.rs   | ~90 +-  (next_buffer / read_line / read_binary)
src/comm.rs     | ~15 +-  (initial rel_path)
src/transfer.rs | ~80 +-  (md5, flush, protocol=1, clean_input,
                          reset_term, term_reseted field)
src/trz.rs      | ~40 +   (RawModeGuard)
src/tsz.rs      | ~40 +   (RawModeGuard)
tests/          | new     (transfer_e2e, protocol_minimal,
                          interop, interop_baseline)
```

## Remaining limitations

- Protocol is V1 only. V2+ requires a streaming encode/decode pipeline
  (zstd / streaming base64 / streaming escape) that hasn't been ported
  yet. Performance is therefore lower than `trzsz-go`.
- The directory-transfer path uses the same `rel_path` initialization
  fix but has not been exercised end-to-end against `trzsz-go`.
- The `unique_id` formula in `tsz.rs` / `trz.rs` uses
  `(timestamp_millis() % 10_000_000_000) * 100` (12 digits, padded to
  13), versus Go's `(UnixMilli() % 10e10) * 100` (13 digits). Both
  satisfy the `\d{13}\d*` regex the filter detector uses, so this is
  cosmetic, not a bug — but worth aligning if the value is ever used
  for cross-host correlation.
