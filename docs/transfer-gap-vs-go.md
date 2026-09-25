# trzsz-rs vs trzsz-go：文件传输链路差距分析

> 实施顺序与里程碑见 [`docs/roadmap.md`](roadmap.md)；客户端/库侧差距见 [`docs/library-porting-checklist.md`](library-porting-checklist.md)。

## 当前进度

- **Roadmap 第 1 步 / 传输 P0：已完成**（实现见提交 `06ab606`）：修复 Latin-1 转义、收发数据零进展、畸形协议行、`-r`、binary 降级及 `tmux_output_junk`。
- 新增 `tests/transfer_p0.rs`，覆盖 14 个无需 Go 工具链的回归场景；`cargo fmt --check` 与 `cargo test` 已通过。
- **仍待处理**：权限保留、普通文件删除、文件句柄关闭、实时进度、Ctrl+C 取消，以及协议 V2+ 能力（见第五节）。
- **真实 Go 互通已部分验证**：从 `./trzsz-go/cmd` 构建后，`tests/interop.rs` 的 Go filter ↔ Rust `tsz` 小文件及 256 KiB 文件用例通过；另用 Go `TrzszFilter.OneTimeUpload` → Rust `trz -b -e` 验证了含 16 个控制/二进制字节的原样落盘。仓库互通测试仍硬编码 `/tmp/go-*`，缺少这些文件时可能静默跳过。

**范围**：只看 `trz`/`tsz` 与对端之间的文件传输协议与实现，即 `src/transfer.rs`、`src/buffer.rs`、`src/escape.rs`、`src/comm.rs`（路径/校验部分）、`src/progress.rs`、`src/trz.rs`、`src/tsz.rs` 的传输流程。
**明确排除**：`trzsz` 包装器（ssh/pty/数据泵）、relay、拖拽上传、zmodem、OSC52、文件选择对话框等非传输项。
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

### 6. 文件权限不保留【P1 待处理】

目录上传实测：NAME 里带 `perm: 0o755`，落盘结果

```
0o755 /tmp/…/subdir          （目录恰好也是 0755，看不出差别）
0o644 /tmp/…/subdir/a.txt    ← 期望 0755
```

Go `doCreateFile`/`doCreateDirectory`（`transfer.go:1016-1048`）用 `perm | 0600` / `perm | 0700` 作为创建 mode，缺省 0644/0755；Rust 用 `fs::File::create` / `create_dir_all`（`transfer.rs:842,860`），只吃 umask，**源文件的 mode 位完全丢失**。

### 7. Ctrl+C 停不下来【P1 待处理】

Go `handleServerSignal`：SIGINT/SIGTERM → `stopTransferringFiles(false)`（`comm.go:568-575`）。Rust 的 ctrlc handler 只往一个**没有任何读取方**的 `AtomicBool` 写（`src/trz.rs:158-162`、`src/tsz.rs:156-160`），`stop_transferring_files`（`transfer.rs:240`）无人调用，`TrzszBuffer` 也没有 stop channel（`transfer.rs:245` 注释写着 `// Signal buffer to stop`，下面是空的）→ **传输中无法中断**，只能等 chunk 超时。

### 8. stop & delete 删不掉文件【P1 待处理】

`delete_created_files`（`transfer.rs:584-594`）只调 `fs::remove_dir_all`，普通文件不会被删（Go 用 `os.RemoveAll`，`transfer.go:745-756`）→ "停止并删除" 只能删目录，文件残留。

### 9. 进度回调链路断了【P1 待处理】

- `ProgressCallback::on_step`（`progress.rs:36,398`）**全项目无调用点**；`send_file_data`/`recv_file_data` 不接 progress 参数（Go 每 chunk 调 `progress.onStep`，`transfer.go:878,911,1176,1191`）。
- `TextProgressBar::new` 零调用，`trz.rs:223` / `tsz.rs:214` 传 `&mut None` → 进度条从不显示。
- 顺带：文件句柄从不 `close()`（`FileWriter::close` 无调用点，Go 是 defer close）。

---

## 二、协议能力缺失（Go = V4，Rust = V1）

`src/transfer.rs:50-55` 自述："Use protocol version 1 for now: V2+ requires streaming encode/decode pipeline which is not yet implemented"，协商时 `min(action.protocol, 1)`（`transfer.rs:473-476`）。

| 能力 | Go | Rust | 用户可见影响 |
|---|---|---|---|
| **压缩** | V2 `COMP` 协商 + zstd 流（`pipeline.go:432-481, 292-338`），带 `isCompressionProfitable` 探测（`comm.go:879-948`） | **无 zstd 依赖**，`-c` 解析后只写进 CFG（`transfer.rs:477-479`），数据从不压缩 | **`-c/--compress` 是空开关**，`auto/yes/no` 行为完全相同 |
| **断点续传** | V3 `HASH` 前缀哈希，每 10MB 一次，seek+truncate 续传（`append.go:38-89,162-321`） | 无任何 `HASH` 处理 | 中断后**从 0 重传**，大文件/差网代价高 |
| **流水线** | V2 并发 read→MD5→encode→send→ack（`pipeline.go:784-1017`），RTT 自适应缓冲 | 严格 stop-and-wait；粗粒度 1024→bufsize 倍增（`transfer.rs:677-685`，阈值与 Go 一致） | 吞吐低于 Go（每个 chunk 一个 RTT 往返） |
| **目录归档** | V4 `archiveSourceFiles` 把同 `path_id` 的多项打包单流（`archive.go:81-94`） | `SourceFile::sub_files()` 恒返回 `&[]`（`comm.rs:643-647`） | 多选目录逐个 NAME/DATA 协商，慢且不原子 |
| **暂停/恢复** | V3 `#DATA:=` / `pausing`/`pauseIdx`（`pipeline.go:340-406`） | 无 | 无法暂停 |
| **隧道 + fork 后台** | `listenForTunnel` 发真实端口 + hello 握手 + `switchToBackground`（`comm.go:991-997`、`transfer.go:154-250`） | 端口写死 `0`（`trz.rs:121` / `tsz.rs:117`）、`listen_for_tunnel` 无人调用、`background()` 返回永不触发的 receiver（`transfer.rs:220-224`，且 `transfer.rs:190` 的接收端当场丢弃） | **`-f` 必然失败**："The client doesn't support fork to background" |
| **tmux 输出脏字节** | tmux 普通模式发送 `tmux_output_junk: true`，对端据此启用 `mayHasJunk` + `stripTmuxStatusLine` | **已修复（M0）**：普通 tmux 模式在 CFG 中发送该键，接收行按类型重同步并剥离 tmux 状态行 | 本地回归覆盖；真实 tmux 场景待验证 |

协议消息与 JSON 字段定义经源码比对；真实 Go 互通已验证 base64 下载及 Go filter → Rust `trz -b -e` binary 上传，真实 tmux 场景尚未覆盖。

---

## 三、其他传输细节差异（代码比对）

| 项 | Go | Rust |
|---|---|---|
| `recvLine` 脏数据重同步 | `mayHasJunk` 时 `LastIndex("#TYPE:")`，否则取最后一个 `#`（`transfer.go:422-431`） | **已修复（P0）**：按期望类型重同步；启用 junk 时剥离 tmux 状态行 | 本地回归覆盖；真实 tmux junk 场景待验证 |
| colon 错误载荷 | zlib+base64 编码并加 `[TrzszError] typ:` 前缀，`fail/FAIL/EXIT` 可解（`comm.go:228-248`） | 纯 base64（`transfer.rs:293`），类型不匹配时不解码 → 错误文案乱码 |
| `\r` 结尾行处理 | append 后检查，若以 `\r` 结尾则截断并继续读（`buffer.go:119-137`） | **append 前**检查 `last()`（`buffer.rs:128-132`）→ 单 chunk `#X:1\r\n` 会留下尾随 `\r` |
| 空 `path_name` | `unmarshalSourceFile` 返回 "Invalid source file"（`comm.go:320-329`） | **已修复（P0）**：空 `rel_path` 返回明确错误，不再索引访问 | 本地回归覆盖 |
| 创建文件 errno 文案 | "No permission to write" / "Is a directory" / "Not a directory"（`transfer.go:1016-1055`） | 只有 `Create file [x] failed: …` |
| 目标目录可写检查 | `faccessat(W_OK)` 按当前 uid（`comm.go:276-290`） | 只看 owner 写位（`comm.rs:224-230`，`!mode & 0o200 != 0` 语义碰巧正确但绕） |
| 进度显示的文件名 | 报源文件名（`transfer.go:1151-1153`） | 报 `local_name`（`transfer.rs:786-788`）→ 改名/目录模式下显示不同 |
| 完成提示 | `"Saved N … to X"` + `"\r\n- "` 分隔（`comm.go:950-978`） | `"No file saved"` + `"\n"` 分隔（`comm.rs:487-510`） |
| 保存路径 | 用 `filepath.Abs` 后的绝对路径 | `recv_files` 传的是**原始相对** `args.path`（`trz.rs:223`，canonicalize 结果只用于校验）→ 提示显示相对路径 |
| `targetFile`（V3 用） | 有校验（`comm.go:336-353`） | 结构定义了但**无使用方** |
| 重名检测 `-y` | `filepath.Join(RelPath...)`（`comm.go:425-435`） | `rel_path.join("/")`（`comm.rs:351-360`，Unix 等价） |

---

## 四、实测通过、与 Go 行为一致的部分

- **目录上传**：`trz -d` 收到 `path_name:["subdir"]` 和 `["subdir","a.txt"]`，正确建出 `subdir/a.txt`，内容与 MD5 全对 ✅（唯一问题是权限）
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
5. ⏳ **P1 待处理** — 创建文件/目录应用 `perm|0600` / `perm|0700`；`delete_created_files` 兼容普通文件；关闭文件句柄；接通 `on_step`。
6. ⏳ **P1 待处理** — Ctrl+C 接入 `stop_transferring_files`（需给 buffer 加 stop channel）。
7. ⏳ **P2 待处理** — V2 流水线 + zstd（让 `-c` 生效）→ V3 断点续传 → V4 archive → 隧道/fork。

---

## 附：本报告的实测手段

- 本轮从 `./trzsz-go/cmd` 构建 Go 工具，`tests/interop.rs` 的两项 Go filter ↔ Rust `tsz` 下载用例实际运行并通过；`interop_baseline` 是 Go ↔ Go 基线，不作为 Rust 互通证据。
- 用 scratch 中的临时 Go probe 调用 `TrzszFilter.OneTimeUpload`，向 Rust `trz -b -e` 上传 16 字节控制/二进制 payload，已逐字节验证一致；probe 未纳入仓库自动测试。
