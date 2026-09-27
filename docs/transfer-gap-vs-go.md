# trzsz-rs vs trzsz-go：文件传输链路差距分析

> 实施顺序与里程碑见 [`docs/roadmap.md`](roadmap.md)；客户端/库侧差距见 [`docs/library-porting-checklist.md`](library-porting-checklist.md)。

## 当前进度

- **Roadmap 第 1 步 / 传输 P0：已完成**（实现见提交 `06ab606`）：修复 Latin-1 转义、收发数据零进展、畸形协议行、`-r`、binary 降级及 `tmux_output_junk`。
- **剩余传输 P1：已完成**：接收文件/目录按 Go 的 `perm | 0600` / `perm | 0700` 创建；停止删除兼容普通文件和目录；发送/接收句柄在成功、失败路径均 close；逐 DATA chunk 调用进度回调，`trz`/`tsz` 接入 stderr 进度条；Ctrl+C 通过共享 stop 状态唤醒 buffer 等待。
- **V3+ 暂停/确认菜单与库侧 Filter 已实现**：`trz`/`tsz` CLI 支持暂停/停止；Rust `TrzszFilter` 提供注入式 `run()`、S/R/D、host selectors、主动/一次性上传及 Phase 5 drag、ZMODEM、OSC52 callback、trace、tmux control mode；公开 `TrzszRelay` API 支持 ACT/CFG、双向中继和可选 tunnel。Go 互通与模拟跳板测试见 `tests/filter_interop.rs`、`tests/relay_interop.rs`。自动 CLI/PTY lifecycle 与 `trzsz -r` 接线不在库移植目标范围。
- V2 流式 zstd 编码/解码及 protocol 1 回退已实现；V2 出站每文件最多 5 帧在途，帧从 10 KiB 起并按 ACK 用时自适应调整到 `min(CFG.bufsize, 64 MiB)`，文本流使用 Base64、binary 流使用 escape，文件结束后继续执行原 MD5 校验。V2 帧大小调节与 Go 的 ACK 阈值对齐；read/encode/send 的完整并行 pipeline 仍与 Go 不同。V3+ 支持 Go 兼容 `#DATA:=` 暂停心跳；V1/V2 不启用暂停。Rust 最高声明 protocol 4。
- **V3 HASH 断点续传已实现**：按 10 MiB 累积 MD5 checkpoint 协商，匹配前缀后从末端继续；摘要不匹配时保留最后一个匹配 checkpoint 并重传其后内容。Rust protocol 3 保留 HASH 前的整数 `SIZE` 行；Go V4 的 HASH 使用 NAME 中的源尺寸、不交换此整数行，Rust V4 已对齐。
- **V4 目录归档已实现**：同一 `path_id` 的目录子项在非覆盖模式下聚合为单个归档 DATA 流；覆盖模式和 protocol 1–3 保持 Go 的非归档行为。
- **真实 Go V4 双向互通已验证**：`tests/v3_go_interop.rs` 自行构建仓库 `trzsz-go` filter/probe，覆盖压缩选择/HASH 续传、目录归档，以及 Go filter 显式设置 tunnel connector 后的 Rust `tsz -f` 下载和 Go `OneTimeUpload` → Rust `trz -f` 上传。
- 回归覆盖：除既有 HASH/归档测试外，`src/transfer.rs` 覆盖本地 TCP tunnel hello/ACT/CFG 切换及 protocol 2/3 暂停能力门控；`src/v2.rs` 覆盖 pause heartbeat、恢复后的文件字节/MD5 完整性和暂停时取消；真实 Go filter probe 覆盖 `trz -f`/`tsz -f` 双向传输。
- **协议边界说明**：Go protocol 2（`Protocol < 3`）固定 `compress = !binary`，不交换 `COMP`，也不应用 CFG `compress`；V3 的 `yes/no` 由 CFG 选择，`auto` 在 size <512 时不压缩、512 B ≤ size <128 KiB 固定压缩，size ≥128 KiB 采样后交换 `#COMP:true/false`。Rust V3 对 Go peer 遵循该规则；Go protocol 2 的 `-c no` 仍无法由 Rust 端覆盖。
- **第三节差异已收敛**：补齐 colon/远端错误载荷、junk 模式 CRLF、当前进程目录写权限、创建 errno 文案、源文件进度名、零文件/绝对路径完成提示；回归覆盖见 `src/buffer.rs`、`src/comm.rs`、`src/transfer.rs`、`tests/transfer_p0.rs` 与 `tests/transfer_p1.rs`。真实 tmux junk 场景仍待验证。
**范围**：只看 `trz`/`tsz` 与对端之间的文件传输协议与实现，即 `src/transfer.rs`、`src/v2.rs`、`src/buffer.rs`、`src/escape.rs`、`src/comm.rs`（路径/校验部分）、`src/progress.rs`、`src/trz.rs`、`src/tsz.rs` 的传输流程。
**报告范围排除**：本文不分析 `trzsz` SSH/PTY wrapper、Relay 与 drag/ZMODEM/OSC52/trace/tmux control mode 的具体实现；这些库侧能力的当前状态见 [`docs/library-porting-checklist.md`](library-porting-checklist.md)。
**参考实现**：`trzsz-go` @ `4432ed0`（子模块已检出）。
**方法**：源码逐函数比对 + 实测 —— 用 Python 按 Go 的线缆格式（`#TYPE:` + base64(zlib(...))）直接驱动 `target/debug/trz` 与 `tsz`，跑 ACT/CFG/NUM/NAME/SIZE/DATA/MD5/EXIT 全流程。

---
## 一、实测功能性缺陷及修复状态
> 以下复现与根因记录的是修复前现象；标题状态及第五节反映当前状态。

### 1. 二进制模式 `-b` 的转义链路【P0 已修复；Go binary 上传互通已验证】

**线上实测**（`trz -b -e` 发出的 CFG）：

```json
"escape_chars": [["\ufffd","\ufffd\ufffd"], ["~","\ufffd1"],
                 ["\u0002","\ufffdA"], ["\r","\ufffdB"], …, ["\ufffd","\ufffdI"] …]
```

根因：`src/trz.rs:206-209` 用 `String::from_utf8_lossy` 序列化原始字节 —— `0xEE/0x8D/0x90/0x91/0x93/0x9D` 是非法 UTF-8，全部变成 U+FFFD。两边后果都不可接受：

- **rs → Go**：Go 的 `escapeCharsToTable`（`trzsz-go/trzsz/escape.go:76-121`）要求每个源字符经 ISO-8859-1 编码后恰好 1 字节、目标恰好 2 字节且首字节为 `0xEE`。U+FFFD 在 Latin-1 之外 → `recvConfig` 直接失败，**二进制上传在客户端侧中止**。
- **rs ↔ rs**：`get_escape_table` 里 `escape_chars_to_table(...).unwrap_or_default()`（`src/transfer.rs:392-402`）静默失败 → 空表。
  **实测证明**：往 `trz -b` 发送含 `1b 5b 41 03 0a 0d ee 7e 11 13` 的原始 payload，服务端逐字节原样落盘（`stored == raw payload: True`，MD5 也匹配）→ **转义完全没生效**。

对比：Go 的 `escapeData` 仅在 table 为 nil/空时 pass-through（`escape.go:135-141`），正常情况下会把 `0xEE/~/0x02…0x9D` 转成 `0xEE + code`，正是为了防止这些控制字节被终端/pty 层解释。Rust 的 `-b`（以及 `-e`）等于**对控制字符不设防**，二进制文件里出现 ESC/0x03 等字节时有真实损坏风险。

### 2. 源文件在 SIZE 协商后变小导致死循环【P0 已修复】

`send_file_data`（`src/transfer.rs:664-686`）里 `n == 0` 时 `step += 0`，循环条件 `step < size` 永远成立。Go 有 EOF 校验（`trzsz-go/trzsz/transfer.go:890-898`）：

```go
if err == io.EOF {
    if length+step != size { return nil, simpleTrzszError("EOF but length… <> size") }
}
```

**实测**：文件在触发后被截断为 0（`SIZE` 仍按元数据发 1000），rs `tsz` 在 **4 秒内发出 99030 个空 `#DATA:` 块**，永不结束、永不报错；对端 `recv_file_data` 同样 `step += 0` → 双向卡死并疯狂刷流。

### 3. 垃圾/损坏输入导致 panic 或无法重同步【P0 已修复】

```
发送 ":oops\n" → panicked at src/transfer.rs:295:28: byte range starts at 1 but ends at 0，exit 101
发送 "#\n"     → #FAIL:…（正常）
发送 "#:\n"    → #FAIL:（空消息）
```

- `recv_check` 缺 Go 的 `idx < 1` 保护（`transfer.go:441`）
- 缺 `mayHasJunk` 重同步（`LastIndex("#TYPE:")`，`transfer.go:422-431`）
- 缺 `stripTmuxStatusLine`（`transfer.go:359-381`）

Go 遇到脏行是**报错或重同步**，Rust 是**崩进程**；且 `Cargo.toml` 里 `panic = "abort"`，release 下直接 abort，**连 `#FAIL` 都发不出去**，对端只能等超时。这正好打在传输最脆弱的场景上（tmux 状态栏、窗口刷新、终端回显插入的垃圾字节）。

### 4. `-r/--recursive` 完全不生效【P1 已修复】

```
tsz -r <目录>  → stderr "Is a directory: …"，exit 255       （-d 正常）
trz -r <目录>  → 触发头 R（应为 D）：::TRZSZ:TRANSFER:R:…
trz -d <目录>  → 触发头 D ✓
```

原因：`src/args.rs:86` 的 `effective_directory()` 只在单测里被引用，`src/trz.rs:120,197,215`、`src/tsz.rs:80,196,206` 全都直接读 `args.base.directory`。Go 在 `trz.go:63-65` / `tsz.go:62-64` 会把 Recursive 归并进 Directory。**`-r` 是纯装饰参数。**

### 5. tmux / Windows 的 binary 自动降级只打印不执行【P1 已修复】

`src/trz.rs:107-112`、`src/tsz.rs:104-109` 打印 "auto switch to base64 mode" 后**没有清 flag**，后面 `let mut binary = args.base.binary` 照样为 true；Go 是 `args.Binary = false`（`trz.go:149-156`）。结果：在 tmux / Windows 下声称降级，实际仍按 binary 走。

### 6. 文件权限不保留【P1 已修复】

目录上传实测：NAME 里带 `perm: 0o755`，落盘结果

```
0o755 /tmp/…/subdir          （目录恰好也是 0755，看不出差别）
0o644 /tmp/…/subdir/a.txt    ← 期望 0755
```

Go `doCreateFile`/`doCreateDirectory`（`transfer.go:1016-1048`）用 `perm | 0600` / `perm | 0700` 作为创建 mode，缺省 0644/0755；Rust 用 `fs::File::create` / `create_dir_all`（`transfer.rs:842,860`），只吃 umask，**源文件的 mode 位完全丢失**。

### 7. Ctrl+C 停不下来【P1 已修复】

Go `handleServerSignal`：SIGINT/SIGTERM → `stopTransferringFiles(false)`（`comm.go:568-575`）。Rust 的 ctrlc handler 只往一个**没有任何读取方**的 `AtomicBool` 写（`src/trz.rs:158-162`、`src/tsz.rs:156-160`），`stop_transferring_files`（`transfer.rs:240`）无人调用，`TrzszBuffer` 也没有 stop channel（`transfer.rs:245` 注释写着 `// Signal buffer to stop`，下面是空的）→ **传输中无法中断**，只能等 chunk 超时。

### 8. stop & delete 删不掉文件【P1 已修复】

`delete_created_files`（`transfer.rs:584-594`）只调 `fs::remove_dir_all`，普通文件不会被删（Go 用 `os.RemoveAll`，`transfer.go:745-756`）→ "停止并删除" 只能删目录，文件残留。

### 9. 进度回调链路断了【P1 已修复】

- `send_file_data` / `recv_file_data` 现在逐 DATA chunk 调 `ProgressCallback::on_step`；`trz` / `tsz` 在非 quiet 模式创建 `TextProgressBar` 并接入回调。
- 发送与接收流程都在整个文件流程成功或失败后调用 reader/writer 的 `close()`；句柄随文件处理结束释放。

---

## 二、协议能力进度（Go = V4，Rust = V4）

`K_PROTOCOL_VERSION` 现为 4；Rust 在 ACT 声明最高 V4，发送 CFG 时取 `min(action.protocol, 4)`。协商为 1 时仍进入原 stop-and-wait V1 路径；V2 提供流式 DATA 与有限 ACK 窗口；V3 增加 HASH 续传、Go 兼容 COMP 协商及暂停/恢复心跳；V4 增加目录归档。隧道/fork 已接入 CLI。

| 能力 | Go | Rust | 用户可见影响 |
|---|---|---|---|
| **V3 COMP 与压缩选择** | V2（`Protocol < 3`）固定 `compress = !binary`，不交换 COMP；V3+ 的 yes/no 从 CFG 选择固定压缩，不发 COMP；auto：size <512 不压缩，512 B ≤ size <128 KiB 固定压缩，size ≥128 KiB 采样并交换 `#COMP:true/false`（`pipeline.go:432-480`、`comm.go:900-948`） | V2 对 Go peer 保持 `!binary`；V3 实现 Go 的 yes/no/auto 阈值、采样和布尔行格式；Rust↔Rust V2 既有 COMP 扩展不变 | V3 的 `-c yes/no/auto` 与 Go 对等；V2 Go peer 的 `-c` 仍不参与协议选择 |
| **V2 数据流水线** | 并发 read→MD5→encode→send→ack；发送块从 10 KiB 起，按 ACK 用时自适应调整至 `CFG.bufsize`；每 DATA 返回 `SUCC:length/step`，以空 DATA 结束并等待最终 step ACK（`pipeline.go:653-767,849-1076`） | 出站最多 5 帧；帧大小按 Go 的 `<500 ms` 翻倍、`≥2 s` 降档规则自适应，范围为 1 KiB 至 `min(CFG.bufsize, 64 MiB)`；入站每帧≤`min(2×CFG.bufsize, 64 MiB)`，文本行在缓冲累积时限长，zstd 解码窗口≤128 MiB；校验逐帧 ACK、结束标记、SIZE 与 MD5 | 高延迟下增大单帧和在途窗口的有效字节数；Rust 尚未并行化 Go 的 read/encode/send 阶段 |
| **V1 回退** | protocol 小于 2 使用旧逐块 DATA/单整数 ACK | 收到 protocol 1 CFG 仍使用原 zlib+Base64 / binary 线格式及 stop-and-wait | 老对端不接收 V2 DATA 流，保持旧行为 |
| **断点续传 HASH** | V3 使用 10 MiB 累积 MD5 checkpoint；V4 仍按同样 checkpoint 匹配，但从 NAME 的源 `size` 取尺寸，不交换整数 `SIZE` 行（`append.go:162-205,255-320`） | **已实现**：protocol 3 保留整数 `SIZE` 行；protocol 4 从 NAME 的 `size` 取源尺寸并省略该行；HASH/SUCC 校验、失配后截断到最后匹配点 | 老 V3 行为不变；Go V4 文件续传与 Go peer 兼容 |
| **V4 目录归档** | V4 非覆盖模式将同 `path_id` 项打包为单个 NAME/DATA；归档流由每项的 base64(zlib(SourceFile JSON)) 行、后接其文件字节构成（`archive.go:81-94,106-185`）。覆盖模式不归档 | **已实现**：非覆盖 V4 按 `path_id` 聚合；归档 DATA reader/writer 维持 Go 线格式，跨任意 chunk 解码；归档不走 HASH，目标 SIZE 为 0；覆盖模式与 protocol 1–3 不聚合 | 目录树语义不变，减少目录中每个子项单独的 NAME/SIZE/MD5 协商 |
| **暂停/确认停止** | Go V3 `#DATA:=` / `pausing`/`pauseIdx`（`pipeline.go:340-406`）；暂停时可停止保留/删除或继续 | CLI 已支持 V3+ 暂停与确认菜单；`TrzszFilter` 核心输入泵独立分流 Ctrl+C/菜单键，空闲输入转发；V1/V2 直接停止 | CLI 复用协议 stdin 时 DATA 帧内 ETX 保持为文件字节；filter 接收端点是 host 提供的独立客户端输入，完整 library transfer worker/菜单运行于 `run()` |
| **隧道 + fork 后台** | loopback listener 在触发头公布端口；`CLIENT/SERVER HELLO` 握手；ACT 标记 tunnel/support fork；CFG fork 后切后台 | **CLI 已实现**：`trz`/`tsz` 触发头公布动态 loopback 端口；验证 Go hello 后将 tunnel 输入并入缓冲区；ACT 协商后切换协议 writer；Unix `-f` fork/setsid，CFG 仅在 tunnel 已连且请求 fork 时置 `fork`/`quiet`。非 CLI/library 的 `TrzszTransfer::background()` channel 仍是桩，未用于此 CLI 路径 | 仓库 Go filter 显式配置 `SetTunnelConnector` 后双向真实传输通过；`-f` 当前仅 Unix 支持，Go 的交互式后台确认/终端菜单不在本次范围 |
| **tmux 输出脏字节** | tmux 普通模式发送 `tmux_output_junk: true`，对端据此启用 `mayHasJunk` + `stripTmuxStatusLine` | **已修复（M0）**：普通 tmux 模式在 CFG 中发送该键，接收行按类型重同步并剥离 tmux 状态行 | 本地回归覆盖；真实 tmux 场景待验证 |

真实 Go V4 双向测试覆盖文件压缩选择、HASH 匹配/不匹配续传、目录归档，以及显式启用 tunnel connector 的 Rust `tsz -f`/`trz -f`。Rust 单测覆盖暂停心跳、恢复后完整性和取消；真实 tmux 场景尚未覆盖。

---

## 三、其他传输细节差异（代码比对）

| 项 | Go | Rust |
|---|---|---|
| `recvLine` 脏数据重同步 | `mayHasJunk` 时 `LastIndex("#TYPE:")`，否则取最后一个 `#`（`transfer.go:422-431`） | **已修复（P0）**：按期望类型重同步；启用 junk 时剥离 tmux 状态行 | 本地回归覆盖；真实 tmux junk 场景待验证 |
| colon / 远端错误载荷 | 缺少冒号时编码 raw line 并用 `colon` 类型构造错误；类型不匹配时解码 `fail/FAIL/EXIT`（`transfer.go:444-452`） | **已修复**：无有效冒号及类型不匹配均按 Go 构造错误；远端错误载荷解码为可读消息，非法编码保持原载荷；普通与限长读取路径均有本地回归测试 |
| `\r` 结尾行处理 | junk 模式下完整行追加后检查尾部 `\r`，截断并继续读（`buffer.go:119-137`） | **已修复**：改为追加后检查；单块与跨块 CRLF 协议行均有本地回归测试 |
| 空 `path_name` | `unmarshalSourceFile` 返回 "Invalid source file"（`comm.go:320-329`） | **已修复（P0）**：空 `rel_path` 返回明确错误，不再索引访问 | 本地回归覆盖 |
| 创建文件 errno 文案 | `EACCES` / `EISDIR` / `ENOTDIR` 分别返回 "No permission to write" / "Is a directory" / "Not a directory"（`transfer.go:1016-1055`） | **已修复**：按对应 `io::ErrorKind` 映射；三种分类均有本地测试 |
| 目标目录可写检查 | `faccessat(W_OK)` 按当前 uid（`comm.go:276-290`） | **已修复**：Unix 使用 `libc::access(W_OK)` 按当前进程权限判定，并有 W_OK 对照回归测试 |
| 进度显示的文件名 | 报源文件名（`transfer.go:1151-1153`） | **已修复**：使用接收 NAME 中的源文件名；本地目录重名映射回归验证 |
| 完成提示 | `Saved N … to X`；零文件仍显示 `Saved 0 file/directory`，列表用 `\r\n- ` 分隔（`comm.go:950-978`） | **已修复**：零文件/文件列表格式与 Go 一致；本地单测覆盖 |
| 保存路径 | 用 `filepath.Abs` 后的绝对路径 | **已修复**：canonicalize 后的绝对目标路径传入接收流程，并用于完成提示 |
| `targetFile`（V3 用） | `NAME` 响应携带目标名和已有文件大小；Go 校验负 size（`comm.go:336-353`） | V3 已解析 target 并用 size 启动 HASH 续传，拒绝负 size |
| 重名检测 `-y` | `filepath.Join(RelPath...)`（`comm.go:425-435`） | `rel_path.join("/")`（`comm.rs:351-360`，Unix 等价） |

---

## 四、实测通过、与 Go 行为一致的部分

- **目录上传**：`trz -d` 收到 `path_name:["subdir"]` 和 `["subdir","a.txt"]`，正确建出 `subdir/a.txt`，内容与 MD5 全对；接收端目录/文件权限按 Go 的 `perm | 0700` / `perm | 0600` 创建 ✅
- **重名不覆盖**：已有 `a.txt` → 落盘 `a.txt.0`，原文件保留 ✅ —— 与 Go `getNewName`（`comm.go:437-453`，`%s.%d`、255 字节上限、最多 1000 次）逐条一致
- **零字节文件**：`SIZE:0` → 不发 DATA 直接 MD5 ✅
- **MD5 逐文件校验**，不匹配报 `Check MD5 failed` ✅
- **自适应块大小**：1024 → <500ms 翻倍 → 封顶 `bufsize`，≥2s 回落 1024 ✅（阈值与 Go 完全一致）
- **超时**：`get_new_timeout` / `clean_input(500ms)` / `err_receive_data_timeout` 语义一致 ✅
- **base64 + zlib 行编码**、`#DATA:<len>\n` 二进制帧、`SUCC` 长度确认 ✅
- **转义表内容**（2 组基础 + `-e` 时 13 个控制字符、`0xEE` leader）逐字节一致；CFG 已改用 Latin-1 编解码并有回归测试 ✅
- **CLI 参数**：10 个 base 参数及默认值保持一致；`-r` 已接入触发头、路径检查和 CFG，并有 CLI 协议回归测试 ✅

---

## 五、优先级与进度（只谈传输）

1. ✅ **P0 完成** — `escape_chars` 使用 Latin-1 单字节语义；binary 转义表无效时明确报错，不再静默降级为空表。
2. ✅ **P0 完成** — 发送源提前 EOF、接收空 DATA chunk 均返回错误；拒绝负长度和超过 SIZE 的数据，避免零进展循环。
3. ✅ **P0 完成** — `recv_check` 安全处理空载荷、冒号位置及非 UTF-8 输入；按期望类型重同步，并剥离 tmux 状态行。保留 release `panic = "abort"`，通过输入校验避免已知传输路径 panic。
4. ✅ **P1 完成** — `effective_directory()` 接入 `trz`/`tsz`；tmux/Windows binary 降级实际生效；普通 tmux CFG 发送 `tmux_output_junk`。
5. ✅ **P1 完成** — 接收权限按 `perm|0600` / `perm|0700` 创建；stop/delete 删除普通文件和目录；文件读写流程在成功/失败路径 close；`on_step` 按 chunk 接入发送/接收并连到 CLI 进度条。
6. ✅ **P1 完成** — Ctrl+C handler 设置 buffer 共享 stop 状态；buffer 以短间隔轮询，使无传输超时的等待也可中止；保留停止但不删除与停止并删除语义。
7. ✅ **P2 / V2 流水线完成** — 流式 zstd、五帧有界 ACK 窗口和 protocol 1 回退；V2 Go peer 仍固定 `!binary`，不收发 COMP。
8. ✅ **P2 / V3 HASH + COMP 完成** — 实现 10 MiB HASH checkpoint 续传和 Go V3+ `yes/no/auto` COMP 规则；Rust protocol 3 的整数 `SIZE` 线格式及 Go V4 的 NAME-size 规则均有回归。
9. ✅ **P2 / V4 目录归档完成** — V4 非覆盖模式按 `path_id` 聚合目录条目并以 Go 兼容归档 DATA 流还原；真实 Go 双向互通及 Rust V3 回退测试通过。
- **P2 完成** — 动态 tunnel 端口、Go hello 握手、协议 writer/input 切换、Unix fork/setsid、V3+ pause/resume heartbeat 与暂停停止菜单；Rust V2–V4 与 Filter Phase 0–3/Phase 5、Relay API 均有实现及本地回归。库级 `TrzszTransfer::background()` 仍是桩，非 Unix fork、自动 `trzsz -r` wrapper 接线、原生 GUI/系统剪贴板 UI 与 Windows runtime 验证仍未包含。

---

## 附：本报告的实测手段

- `tests/interop.rs` 仍依赖 `/tmp/go-*` 和本机配置，可能按既有条件跳过；`tests/v3_go_interop.rs` 自行构建仓库 `trzsz-go` filter/probe，不使用固定路径，真实验证 Go V4 文件（COMP/HASH）与目录归档双向互通；Go 工具链缺失时明确失败，不静默跳过。
- 早期 scratch Go probe 对 `TrzszFilter.OneTimeUpload` → Rust `trz -b -e` 的 16 字节控制/二进制 payload 已逐字节验证；协议 1/3 回退、本次 V4 HASH 和目录归档均另有本地回归。
