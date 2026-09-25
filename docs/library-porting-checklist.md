# trzsz-rs 库化路线：Go 库 API → Rust 目标 API 移植清单

> 实施顺序与里程碑见 [`docs/roadmap.md`](roadmap.md)；服务端传输差距见 [`docs/transfer-gap-vs-go.md`](transfer-gap-vs-go.md)。

**背景**：`trzsz-go` 不只是 CLI，还是被真实项目依赖的 Go 库（pkg.go.dev "Imported by" 3 个模块 / 6 个包：`trzsz/trzsz-ssh/tssh`、`abakum/{cssh,dssh,trzsz-ssh}/tssh`、`jixishi/SerialTerminalForWindowsTerminal`、`shoaibashk/nanocom`），核心价值是 `TrzszFilter` —— 让宿主终端程序在本地侧具备触发上传/下载的能力。
`trzsz-rs` 结构上同样是 lib + bin（`src/lib.rs` 11 个 `pub mod`，`Cargo.toml` 无 `[lib]` 段、无 `publish = false`），但客户端过滤器层是空壳。

**本文目的**：按依赖顺序列出从 Go 库 API 到 Rust 的移植清单，每项标注前置条件与验收标准。

**引用约定**：`filter.go:121` 指 `trzsz-go/trzsz/filter.go` 第 121 行；`filter.rs:73` 指 `src/filter.rs` 第 73 行。

---

## Phase 0 — 先定 API 形态（决策，不写功能代码）

Go 的 API 有一个隐含设计：**`NewTrzszFilter` 内部直接 `go filter.wrapInput()` + `go filter.wrapOutput()` 启动数据泵**（`filter.go:121-122`），调用方拿到对象时已经在工作。Rust 里 `TrzszFilter::new`（`filter.rs:73-96`）只构造字段，`trzsz_main` 读完配置就让它在分支结束时被 drop（`trzsz.rs:78-128`）。

Rust 需要显式化生命周期，三选一：

| 方案 | 形态 | 评价 |
|---|---|---|
| A. 显式 `run()` | `fn run(&mut self) -> Result<()>` 阻塞泵；或 `fn spawn(self) -> JoinHandle` | ✅ 最贴近 Rust 习惯，退出/错误可传播（Go 侧错误只能靠 callback） |
| B. `Drop` 里启动 | 构造即工作 | ❌ 反直觉，且无法报告启动失败 |
| C. 回调注册后自启 | 保留 Go 语义 | ⚠️ 需要 `Arc<Self>`，字段共享结构要重排 |

**建议 A**：`new()` 只构造 → `run()` 泵（内部起 2 条线程，对应 Go 的两个 goroutine）→ `close()` 优雅收尾。

同时 Phase 0 要做的收敛决策：

1. **公共边界**：现在 `pub` 到处都是（`TrzszTransfer`、`TrzszBuffer`、内部字段全 `pub`）。库化前应把内部类型改为 `pub(crate)`，只 re-export 目标 API，否则一发布就锁死内部重构。
2. **`panic = "abort"`**（`Cargo.toml` release 段）与库形态**冲突**：宿主程序（tssh 这类）需要 unwind 才能 recover。传输路径上已有实测 panic（`transfer.rs:295` 对 `:oops` 行）。→ 库形态必须去掉 `abort`，或至少保证传输路径不 panic。
3. **crate 元数据**：缺 `repository`/`keywords`/`categories`/crate-level `//!` 文档；不打算发 crates.io 就加 `publish = false`。
4. **与 Go 的参数化差异**：Rust 已把四个端点作为 `Box<dyn Read/Write + Send>` 注入（`filter.rs:57-60`），比 Go 更易测试 —— 保留这个设计，别退回 `os.Stdin` 直接依赖。

### 目标 API 草图（Phase 0 的产出物）

```rust
pub struct TrzszOptions { /* 与 filter.rs:35-41 字段一致 */ }

pub struct TrzszFilter { /* 私有字段 */ }

impl TrzszFilter {
    pub fn new(
        client_in: Box<dyn Read + Send>,
        client_out: Box<dyn Write + Send>,
        server_in: Box<dyn Write + Send>,
        server_out: Box<dyn Read + Send>,
        options: TrzszOptions,
    ) -> Self;

    pub fn run(&mut self) -> Result<(), TrzszError>;   // 对应 Go 的隐式 goroutine 启动
    pub fn close(&mut self);                            // 对应 Go v1.2.0 新增的 Close()

    // 状态查询
    pub fn is_transferring_files(&self) -> bool;
    pub fn set_terminal_columns(&self, columns: i32);
    pub fn stop_transferring_files(&self, stop_and_delete: bool);

    // 宿主联动（Go 侧均在 v1.2.0 为嵌入方新增）
    pub fn set_transfer_state_callback(&self, cb: fn(bool));
    pub fn set_redraw_screen_func(&self, cb: fn());
    pub fn set_tunnel_connector(&self, connector: fn(i32) -> Option<TcpStream>);

    // 路径配置
    pub fn set_default_upload_path(&mut self, path: &str);
    pub fn set_default_download_path(&mut self, path: &str);
    pub fn set_drag_file_upload_command(&mut self, cmd: &str);
    pub fn set_progress_color_pair(&mut self, pair: &str);

    // 主动上传
    pub fn upload_files(&self, paths: &[PathBuf]) -> Result<(), TrzszError>;
    pub fn one_time_upload(&self, paths: &[PathBuf])
        -> (Receiver<Result<(), TrzszError>>, Result<(), TrzszError>);

    pub fn read_trzsz_config(&mut self);
}

pub fn trzsz_main(args: &TrzszArgs) -> i32;   // 已有，对应 TrzszMain()
pub fn trz_main(args: &TrzArgs) -> i32;       // 已有，对应 TrzMain()
pub fn tsz_main(args: &TszArgs) -> i32;       // 已有，对应 TszMain()
pub fn set_affected_by_windows(affected: bool); // 已有，对应 SetAffectedByWindows
```

---

## Phase 1 — 数据泵（P0，库"能用"的最小闭环）

**前置**：Phase 0 的 API 决策；无代码依赖。
**缺失现状**：`src/` 里只有 `trz.rs:142`、`tsz.rs:140` 两处 `thread::spawn`，没有任何代码在 stdin↔pty↔stdout 之间搬运字节；`detect_trzsz`（`filter.rs:152`）除单测外零调用。

| # | 目标 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 1.1 | `wrap_input` + `send_input` | `filter.go:628-707` | ❌ | 读 `client_in` → 写 `server_in`；传输进行时改写给 transfer；处理 Windows EOF→Ctrl+Z、上传命令回显抑制、warp 延迟 |
| 1.2 | `wrap_output` | `filter.go:709-799` | ❌ | 读 `server_out` → `detect_trzsz` → 触发 1.3；同时负责写 `client_out` |
| 1.3 | `handle_trzsz` | `filter.go:503-561` | ❌ | 按触发 mode（`S`/`R`/`D`）分发到下载/上传；panic 恢复；`transferStateCallback`；`background()` select |
| 1.4 | `download_files` | `filter.go:435-465` | ❌ | `send_action` → `recv_config` → 建进度条 → `recv_files` → `client_exit` |
| 1.5 | `upload_files` | `filter.go:466-501` | ❌ | 同上，反向 |
| 1.6 | `detect_trzsz` 加固 | `comm.go:712-815` | ⚠️ 有但弱 | 补：长度≥24、正则校验、**用 `LastIndex` 而非 `find`**、`#CFG:/Saved/Cancelled` 误判保护、**`TRZSZ`→`TRZSZGO` 重写**（缺了会嵌套重复触发）、重复 uniqueID 去重、`win_server` 判据改为 `id=="1" \|\| (13位 && 以"10"结尾)`（`filter.rs:165` 现在的 `ends_with('0')` 会把几乎所有 id 判成 Windows）、tmuxcc prefix/paneID 解析 |
| 1.7 | 关闭/退出语义 | `filter.go:279` `Close()` | ❌ | 停线程、恢复终端、排空 buffer |

**验收**：
- 新增集成测试方向：**rs filter ↔ 真实 Go `trz`/`tsz`**（现在 4 个 interop 测试全是 server 方向，且因缺 Go 工具链静默 skip）
- `trzsz ssh <host>` 能完成一次双向传输（需要 Go 工具链的 CI job）
- 单测对齐 `comm_test.go:157 TestTrzszDetector` / `:294 TestRelayDetector` 的用例集

**注意**：Phase 1 先跑 base64 模式即可；binary 模式必须先修 P0 的 `escape_chars` 编码（见 `docs/transfer-gap-vs-go.md` 第一节第 1 条），否则 `trz -b` 的 CFG 在 Go 侧解析失败。

---

## Phase 2 — 进度显示与资源管理（P0，"能用"→"好用"）

**前置**：Phase 1.4 / 1.5。

| # | 目标 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 2.1 | `on_step` 接进数据循环 | `transfer.go:878,911,1176,1191` | ❌ `progress.rs:36,398` 有定义无调用 | `send_file_data`/`recv_file_data` 要加 `progress` 参数（签名变更，`trz.rs`/`tsz.rs` 同步）；否则进度条只会 0→100 跳变 |
| 2.2 | `create_progress_bar(quiet, tmux_pane_width)` | `filter.go:415-426` | ❌ `TextProgressBar::new` 零调用 | 收到 CFG 后按 `quiet` 创建 |
| 2.3 | `reset_progress_bar` | `filter.go:428-433` | ❌ | 传输结束清屏/显示光标 |
| 2.4 | 进度显示文件名用源名 | `transfer.go:1151-1153` | ⚠️ 用 `local_name`（`transfer.rs:786`） | 改名 `x.0` / 目录模式下显示不一致 |
| 2.5 | 文件句柄 `close()` | `transfer.go:1173` defer | ❌ `FileWriter::close` 无调用点 | 与 2.1 一起改，避免 fd 泄漏 |
| 2.6 | `on_step` 之外的 EOF 校验 | `transfer.go:890-898` | ❌ | 与 2.1 同一轮改（死循环实测：4 秒 99030 个空 DATA 块） |

**验收**：下载/上传过程中进度条实时推进；`-q` 不显示；传输结束无残留。

---

## Phase 3 — 宿主联动与交互（P1）

**前置**：Phase 1（回调在 `handle_trzsz` 里触发）、Phase 2。

| # | 目标 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 3.1 | `set_transfer_state_callback` | `filter.go:270` (v1.2.0) | ❌ | 嵌入方 UI 联动（忙状态/禁用按钮） |
| 3.2 | `set_redraw_screen_func` | `filter.go:261` (v1.2.0) | ❌ | 传输完成后重绘宿主界面 |
| 3.3 | Ctrl+C → `stop_transferring_files` | `comm.go:568-575`、`ctrlc.go` | ⚠️ `trz.rs:158` 写入无人读的 `AtomicBool` | 需要给 `TrzszBuffer` 加 stop channel（Go `buffer.go:34,50-55 stopBuffer`），否则阻塞读叫不醒 |
| 3.4 | `stop_and_delete` 真删文件 | `transfer.go:745-765` | ⚠️ `transfer.rs:584` 只 `remove_dir_all` | 普通文件删不掉 |
| 3.5 | SIGWINCH → `set_terminal_columns` | `trzsz.go:185`、`pty_unix.go:65-88` | ❌ 全项目无 SIGWINCH；PTY 写死 24×80（`trzsz.rs:152-157`） | 嵌入方通常自己有 resize 通道，暴露 `set_terminal_columns` 即可 |
| 3.6 | 默认上传/下载路径 | `filter.go:209-231` + `.trzsz.conf` | ⚠️ 只存字段（`filter.rs:134-139`），无 `~` 展开、无使用方 | 先做"有默认路径就不弹窗" |
| 3.7 | 文件选择对话框 | `filter.go:327-413`（zenity） | ❌ | 计划里用 `rfd`；**注意嵌入方可能不希望弹窗** → 必须受 3.6 兜底约束，且给 `TrzszOptions` 加开关 |
| 3.8 | `set_progress_color_pair` | `filter.go:247` + `.trzsz.conf:progresscolorpair` | ❌ 配置键都没读（`filter.rs:133-144`） | 与 Phase 2 一起 |

**验收**：宿主程序（可写一个小 demo binary）能注册回调、取消传输、 resize 后进度条宽度正确。

---

## Phase 4 — Relay（P1）

**前置**：Phase 1（复用 detector/handshake 的消息读写）；隧道相关依赖 Phase 6.4。

| # | 目标 | 对应 Go | 现状 |
|---|---|---|---|
| 4.1 | `NewTrzszRelay` + `run` | `relay.go:690` | ❌ `trzsz.rs:78-98` 的 `-r` 分支只是构造了一个普通 filter |
| 4.2 | `handshake`（ACT/CFG 透传） | `relay.go:429-480` | ❌ |
| 4.3 | `wrapInput`/`wrapOutput` 中继 | `relay.go:501-596` | ❌ |
| 4.4 | tunnel relay（hello token 重写） | `relay.go:103-230, 598-688` | ❌ |
| 4.5 | `SetTunnelConnector` / `SetTransferStateCallback` on relay | `relay.go:81,90` | ❌ |
| 4.6 | `Close()` | `relay.go:99` | ❌ |

**验收**：`trzsz -r` 经跳板机完成一次传输（可用 Python 模拟跳板两侧）。

---

## Phase 5 — 可选特性（P2，开关已存在但全是死的）

**前置**：Phase 1。

| # | 开关 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 5.1 | `-d/--dragfile` | `drag.go`(360) + `filter.go:572-626,649-673` | ⚠️ 选项存了（`trzsz.rs:104`）、conf 键读了，无检测逻辑 | 需要 shlex 拆分 + 平台路径（Windows/MSYS/Cygwin/cygpath） |
| 5.2 | `-z/--zmodem` | `zmodem.go`(404) | ⚠️ `trzsz.rs:106` 死开关 | 需要 spawn `sz`/`rz` + 双向流 + 定时器 |
| 5.3 | `-o/--osc52` | `filter.go:801-860` | ⚠️ `trzsz.rs:107` 死开关 | |
| 5.4 | `-t/--tracelog` | `comm.go:817-877 traceLogger` | ⚠️ `trzsz.rs:105` 死开关 | 含 `<ENABLE_TRZSZ_TRACE_LOG>` 协议 |
| 5.5 | tmux control mode 传输 | `tmuxcc.go`(431) | ❌ 仅 `progress.rs:232-248` 有 prefix 编码（但 `tmux_prefix` 恒为空） | 与服务端 `tmux_output_junk`/tty 输出一起做才完整 |
| 5.6 | Windows VT/代码页 | `pty_windows.go`(280)、`setupVirtualTerminal` | ⚠️ `trzsz.rs:131-137` 空函数；`conpty` 依赖声明了未引用 | 库形态下宿主可能自己管，先给 hook |

---

## Phase 6 — 协议 V2+（与库形态正交，可并行推进）

**前置**：无（与 Phase 1-5 无依赖关系，只影响传输性能/能力）。

| # | 目标 | 对应 Go | 现状 |
|---|---|---|---|
| 6.1 | V2 流水线 + `COMP` 协商 + zstd | `pipeline.go`(1076) | ❌ `K_PROTOCOL_VERSION = 1`（`transfer.rs:55`）；**`-c/--compress` 是空开关** |
| 6.2 | V3 前缀哈希断点续传 | `append.go`(375) | ❌ 无 `HASH` 处理 |
| 6.3 | V4 archive 目录打包 | `archive.go`(249) | ❌ `sub_files()` 恒 `&[]`（`comm.rs:643`） |
| 6.4 | 隧道 + fork 后台 | `comm.go:991-997`、`transfer.go:154-250,243` | ⚠️ `listen_for_tunnel` 无人调用、端口写死 0、`background()` 是桩（`transfer.rs:220-224`）、`-f` 必然失败 |
| 6.5 | 暂停/恢复 | `pipeline.go:340-406` | ❌ |

**顺序建议**：6.1 → 6.2 → 6.3 → 6.4 → 6.5。6.1 是 6.2/6.3/6.5 的地基（它们都建立在 V2 的流水线读写上）。

---

## 横切：Phase 1-3 期间必须一起修的传输正确性项

这些不是库 API，但库一旦被嵌入就会被放大（宿主无法自行兜底），清单见 `docs/transfer-gap-vs-go.md` 第一节，此处只列顺序：

1. `escape_chars` Latin-1 编码（`trz.rs:206-209` + `escape.rs:115-127`）→ 否则 binary 模式库用户直接失败
2. `send_file_data` EOF 校验 → 否则死循环
3. `recv_check` `idx<1` 保护 + panic 兜底 + `mayHasJunk` 重同步 / `stripTmuxStatusLine` → 否则脏输入崩掉宿主（库场景下等于把宿主一起带走）
4. `effective_directory()` 接进 `trz.rs`/`tsz.rs` → `-r` 修复
5. binary 降级提示后真正清 flag；`send_config` 发 `tmux_output_junk`
6. 目录/文件 `perm|0600` / `perm|0700`；`delete_created_files` 兼容文件

---

## 测试策略（贯穿各 Phase）

现状缺口：4 个 interop 测试**全在 server 方向**，且因本机无 Go 工具链**静默 skip**；无上传方向、目录、binary、重名、空文件的回归测试。

| 层级 | 计划 |
|---|---|
| 单测 | 对齐 Go：`comm_test.go:157/294`（detector）、`filter_test.go:32`（OSC52）、`progress_test.go`（17 项渲染）、`buffer_test.go:156`（Windows 行模式）、`pipeline_test.go`（V2+zstd，Phase 6） |
| 协议层 | 复用 `tests/protocol_minimal.rs` 的手法，补**客户端方向**（rs filter ↔ Python/Go 服务端） |
| 真实互通 | CI 加 Go 工具链 job：构建 `/tmp/go-trzsz`、`/tmp/go-trz`、`/tmp/go-tsz`，让 `tests/interop*.rs` 真正跑起来；新增 `interop_upload.rs`（Go filter → rs `trz`） |
| 库级 | Phase 3 验收用的 demo 宿主（最小 terminal 程序）作为 `examples/` |

---

## 里程碑建议

| 里程碑 | 内容 | 出口判据 |
|---|---|---|
| **M1 库可用** | Phase 0 + Phase 1（base64 模式）+ 横切 1/2/3 | rs filter 与 Go `trz`/`tsz` 双向真实互通；脏输入不崩 |
| **M2 体验对齐** | Phase 2 + 横切 4/5/6 | 进度条实时、`-r` 生效、权限保留、binary 可用 |
| **M3 宿主接入** | Phase 3 | demo 宿主完成回调/取消/resize 三件事 |
| **M4 中继** | Phase 4 | `trzsz -r` 走通跳板 |
| **M5 特性补齐** | Phase 5 | 按需排期 |
| **M6 性能/能力** | Phase 6 | `-c` 生效、断点续传 |

**关键路径**：Phase 0 → Phase 1.1-1.6 → Phase 2.1 → Phase 3.3。其余（relay、drag、zmodem、协议 V2+）都是可并行的分支。
