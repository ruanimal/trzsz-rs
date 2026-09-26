# trzsz-rs 库化路线：Go 库 API → Rust 目标 API 移植清单

> 实施顺序与里程碑见 [`docs/roadmap.md`](roadmap.md)；服务端传输差距见 [`docs/transfer-gap-vs-go.md`](transfer-gap-vs-go.md)。

**背景**：`trzsz-go` 不只是 CLI，还是被真实项目依赖的 Go 库（pkg.go.dev "Imported by" 3 个模块 / 6 个包：`trzsz/trzsz-ssh/tssh`、`abakum/{cssh,dssh,trzsz-ssh}/tssh`、`jixishi/SerialTerminalForWindowsTerminal`、`shoaibashk/nanocom`），核心价值是 `TrzszFilter` —— 让宿主终端程序在本地侧具备触发上传/下载的能力。
`trzsz-rs` 结构上同样是 lib + bin；注入式 Filter Phase 0–3、Relay API Phase 4 与 Filter 可选能力 Phase 5 已实现，V2–V4 协议层也已存在。自动 SSH/PTY/CLI（含 `trzsz -r`）接线、原生 GUI/系统剪贴板 UI 和库级后台传输不在本清单交付范围；`TrzszTransfer::background()` 仍是桩。

**本文目的**：按依赖顺序列出从 Go 库 API 到 Rust 的移植清单，每项标注前置条件与验收标准。

**引用约定**：`filter.go:121` 指 `trzsz-go/trzsz/filter.go` 第 121 行；`filter.rs:73` 指 `src/filter.rs` 第 73 行。

---

## Phase 0 — API 形态决策（已收敛）

Go 的 `NewTrzszFilter` 在构造时启动两个 goroutine；Rust 明确采用 `new()` 构造 → `run()` 阻塞运行 → `close()` 收尾。该核心生命周期与 Phase 1–3 已在 `src/filter.rs` 实现；宿主应在专用线程运行 filter，并在关闭阻塞 reader 时通过 shutdown handler 唤醒它。

Phase 0 决策：filter 内部字段保持私有；四个 I/O 端点仍由宿主注入；release `panic = "abort"` 不改，filter 传输路径对协议输入做校验；不扩展 crate 发布元数据。

### 已实现的核心 API

```rust
pub struct TrzszFilter { /* private state */ }

impl TrzszFilter {
    pub fn new(client_in: Box<dyn Read + Send>, client_out: Box<dyn Write + Send>,
               server_in: Box<dyn Write + Send>, server_out: Box<dyn Read + Send>,
               options: TrzszOptions) -> Self;
    pub fn run(&self) -> io::Result<()>; // blocking, single-use
    pub fn close(&self);
    pub fn set_shutdown_handlers(&self, client_input: Option<Arc<dyn Fn() + Send + Sync>>,
                                 server_output: Option<Arc<dyn Fn() + Send + Sync>>);
    pub fn is_transferring_files(&self) -> bool;
    pub fn stop_transferring_files(&self, stop_and_delete: bool);
    pub fn set_terminal_columns(&self, columns: i32);
    pub fn set_default_upload_path(&self, path: impl AsRef<Path>);
    pub fn set_default_download_path(&self, path: impl AsRef<Path>);
    pub fn set_upload_path_selector<F>(&self, selector: F); // host supplies paths
    pub fn set_download_path_selector<F>(&self, selector: F); // host supplies directory
    pub fn set_transfer_state_callback<F>(&self, callback: F);
    pub fn set_redraw_screen_func<F>(&self, callback: F);
    pub fn set_progress_callback<F>(&self, callback: F);
    pub fn upload_files(&self, paths: &[PathBuf]) -> Result<(), TrzszError>;
    pub fn one_time_upload(&self, paths: &[PathBuf])
        -> Result<Receiver<Result<(), String>>, TrzszError>;
    pub fn read_trzsz_config(&self);
}
```

Upload selector 接收“是否允许目录”和建议起始目录；download selector 接收建议目录。`Ok(None)` 表示用户取消。默认 download path 用作实际保存目录；默认 upload path 仅作为 picker 起始目录。没有 selector 时不弹 GUI，而是向对端发送取消。

---

## Phase 1 — 数据泵（核心双向传输已实现）

**状态**：Phase 0 API 决策已收敛；输入/输出 reader + dispatcher + 独立 transfer worker 已实现。

| # | 目标 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 1.1 | `wrap_input` + `send_input` | `filter.go:628-707` | ✅ | 空闲字节原样送到 `server_in`；传输时分流 Ctrl+C/暂停菜单键；EOF 关闭服务器输入。可控 blocking reader 用 shutdown handler 唤醒 |
| 1.2 | `wrap_output` | `filter.go:709-799` | ✅ | `server_out` 普通输出送 `client_out`；trigger marker 可跨 read；有效 header 改写为 `TRZSZGO` 防止嵌套重复触发 |
| 1.3 | `handle_trzsz` | `filter.go:503-561` | ✅ | `S`/`R`/`D` 使用独立 transfer worker；状态 callback 在开始/完成时触发 |
| 1.4 | `download_files` | `filter.go:435-465` | ✅ | `ACT`/`CFG`/进度/`recv_files`/`EXIT`，支持默认下载目录、host picker 和取消 |
| 1.5 | `upload_files` | `filter.go:466-501` | ✅ | host picker、默认起始目录、主动 `upload_files` 与 `one_time_upload` 队列；支持目录 |
| 1.6 | `detect_trzsz` 加固 | `comm.go:712-815` | ✅ 核心 | 校验 mode/version/id/port、畸形行原样透传、分片 marker 重组、嵌套触发改写、Go `win_server` 判据；tmuxcc 与 repeated-ID policy 未纳入本目标 |
| 1.7 | 关闭/退出语义 | `filter.go:279` `Close()` | ✅ | `run()` 单次阻塞运行；显式 close/EOF/I/O error 停止主循环；host 可注册 shutdown handlers 唤醒任意 blocking reader |

**验收**：`tests/filter_interop.rs` 构建仓库 Go `tsz`/`trz`，覆盖 S 下载、R 文件上传、D 目录上传及 picker 取消；filter 局部测试覆盖分片/畸形 trigger、透传、close 和 I/O error。Relay 检测与 `trzsz ssh` 包装器不属于此 Phase。

**说明**：filter 复用已完成的 V1–V4 transfer 协议实现，包括 binary、zstd、HASH 与 V4 archive；本次没有更改线缆格式。

---


## Phase 2 — 进度显示与资源管理（核心已实现）

**状态**：传输层逐 DATA chunk 提供 `ProgressCallback`；filter 根据 CFG quiet 创建/清理 `TextProgressBar`，同时向宿主发送 `ProgressEvent`。文件句柄、EOF 校验和源文件名在共享 transfer 层已修复。

| # | 目标 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 2.1 | `on_step` 接进数据循环 | `transfer.go:878,911,1176,1191` | ✅ | 上传/下载逐 chunk 更新，宿主 callback 可观察字节步进 |
| 2.2 | `create_progress_bar(quiet, tmux_pane_width)` | `filter.go:415-426` | ✅ | 收到 CFG 后按 quiet 和 terminal columns 初始化 |
| 2.3 | `reset_progress_bar` | `filter.go:428-433` | ✅ | worker 成功、取消、失败均恢复光标并释放进度状态 |
| 2.4 | 进度显示文件名用源名 | `transfer.go:1151-1153` | ✅ | transfer 层已使用源文件名 |
| 2.5 | 文件句柄 `close()` | `transfer.go:1173` defer | ✅ | 传输文件读写器在成功/错误路径关闭 |
| 2.6 | EOF 校验 | `transfer.go:890-898` | ✅ | 文件提前 EOF 返回错误，不再空转 |

**验证**：Go `tsz` → Rust filter 集成测试检查 progress callback、完成事件和光标清理；quiet CFG 下不创建可见进度条。

---


## Phase 3 — 宿主联动与交互（核心 API 已实现）

**状态**：宿主可注册 transfer-state/redraw/progress callback、停止或删除当前传输、更新 terminal columns，并配置路径 picker/default path。

| # | 目标 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 3.1 | `set_transfer_state_callback` | `filter.go:270` (v1.2.0) | ✅ | 成功、取消、失败均成对发出开始/结束状态 |
| 3.2 | `set_redraw_screen_func` | `filter.go:261` (v1.2.0) | ✅ | worker 完成后调用 |
| 3.3 | Ctrl+C / `stop_transferring_files` | `comm.go:568-575`、`ctrlc.go` | ✅ | filter 输入泵分流 Ctrl+C 与暂停菜单；宿主可显式停止，stop state 唤醒 buffer |
| 3.4 | `stop_and_delete` | `transfer.go:745-765` | ✅ | transfer 层支持删除文件和目录 |
| 3.5 | `set_terminal_columns` | `trzsz.go:185` | ✅ API | resize 信号处理由宿主注册后调用，不由 filter 安装全局 signal handler |
| 3.6 | 默认上传/下载路径 | `filter.go:209-231` + `.trzsz.conf` | ✅ | `~` 展开；下载默认值是保存目录，上传默认值作为 picker 起始目录 |
| 3.7 | 文件选择 | `filter.go:327-413` | ✅ host API | picker callback 提供路径并可取消；不内置 GUI，嵌入方决定 UI |
| 3.8 | `set_progress_color_pair` | `filter.go:247` + `.trzsz.conf` | ⏭ | 可选展示项，本次范围排除 |

**验证**：`tests/filter_interop.rs` 验证状态/重绘/进度 callback、host selector 起始路径及上传/下载取消；单元测试覆盖 close 和 I/O error。未新增 demo terminal host。

---

---

## Phase 4 — Relay（P1）

**前置**：Phase 1（复用 detector/handshake 的消息读写）；隧道相关依赖 Phase 6.4。

| # | 目标 | 对应 Go | 状态 | 说明 |
|---|---|---|---|---|
| 4.1 | Relay 构造与 `run` | `relay.go:690` | ✅ | 新增并导出 `TrzszRelay::new/run`，单次阻塞运行；CLI `-r` 生命周期接线仍不在本目标范围 |
| 4.2 | ACT/CFG 握手 | `relay.go:429-480` | ✅ | 编解码并转发 ACT/CFG，协商协议上限与 binary 能力；畸形输入以 FAIL 返回 |
| 4.3 | 双向输入/输出中继 | `relay.go:501-596` | ✅ | 缓冲握手前字节，双向转发正文，EXIT/FAIL 结束状态并成对通知 callback |
| 4.4 | tunnel relay（hello/token 重写） | `relay.go:103-230, 598-688` | ✅ | 动态 loopback listener、触发端口重写、两段 hello/token 校验与双向隧道转发；覆盖 loopback 回归 |
| 4.5 | `SetTunnelConnector` / `SetTransferStateCallback` | `relay.go:81,90` | ✅ | Rust `set_tunnel_connector` 注入服务器连接，`set_transfer_state_callback` 通知起止状态 |
| 4.6 | `Close()` | `relay.go:99` | ✅ | `close` 停止 listener，并通过 host shutdown handlers 唤醒阻塞 reader；有单测覆盖 |

**验收**：`tests/relay_interop.rs` 通过 Relay + Rust Filter 向仓库 Go `trz` 完成文件上传；`relay.rs` 单测覆盖 ACT/CFG、错误、普通中继、loopback hello 隧道与 close。自动 `trzsz -r` CLI/PTY 接线按本目标边界未包含。


---

## Phase 5 — 可选特性（P2，Filter API 已实现）

**前置**：Phase 1。

| # | 开关 | 对应 Go | 现状 | 说明 |
|---|---|---|---|---|
| 5.1 | `-d/--dragfile` | `drag.go`(360) + `filter.go:572-626,649-673` | ✅ | bounded fragmented input + shell-word parsing；POSIX/bracketed paste/Warp 及 Windows drive/MSYS/Cygwin/可选 `cygpath` 路径；回归见 `filter/features/drag.rs`，实际路径经 metadata 校验 |
| 5.2 | `-z/--zmodem` | `zmodem.go`(404) | ✅ | 检测 init/finish，host selector 选路径，启动可配置 `sz`/`rz`，双向泵、OverAndOut、cancel 与 20 秒 inactivity timeout；假子进程回归见 `filter/features/zmodem.rs` |
| 5.3 | `-o/--osc52` | `filter.go:801-860` | ✅ | 分片/限长 OSC52 parser 解码 Base64，通过 `set_clipboard_callback` 交给宿主；不内置平台剪贴板依赖 |
| 5.4 | `-t/--tracelog` | `comm.go:817-877 traceLogger` | ✅ | `<ENABLE_TRZSZ_TRACE_LOG>` / `<DISABLE_TRZSZ_TRACE_LOG>` 跨 read 识别、临时日志、Go 兼容 `[type]base64(zlib(bytes))` 记录；有实际日志文件回归 |
| 5.5 | tmux control mode 传输 | `tmuxcc.go`(431) | ✅ | pane/output 分片解码、octal 数据、`send -lt/-t` 与 ack、ACT `tmuxcc` 协商、tmux-prefixed progress；单测覆盖协议编码和 ack |
| 5.6 | Windows VT/代码页 | `pty_windows.go:78-160` | ✅ API | 导出 `WindowsConsoleGuard` RAII hook，保存/恢复 mode 与 UTF-8 code pages，启用 VT input/output 和 `DISABLE_NEWLINE_AUTO_RETURN`；Windows 模块独立 cross-check 通过，macOS no-op/纯逻辑测试通过，Windows runtime 未运行 |

---

## Phase 6 — 协议 V2+（与库形态正交，可并行推进）

**前置**：无（与 Phase 1-5 无依赖关系，只影响传输性能/能力）。

| # | 目标 | 对应 Go | 状态 | 说明 |
|---|---|---|---|---|
| 6.1 | V2 流水线 + `COMP` 协商 + zstd | `pipeline.go`(1076) | ✅ | 有界 V2 DATA 窗口、zstd 流编码/解码、COMP 协商和 protocol 1 回退已实现；Go V2 对端保持固定 `!binary` 行为 |
| 6.2 | V3 前缀哈希断点续传 | `append.go`(375) | ✅ | 10 MiB HASH checkpoint 匹配/回退重传已实现；V3/V4 SIZE 差异与 Go 互通有回归覆盖 |
| 6.3 | V4 archive 目录打包 | `archive.go`(249) | ✅ | V4 非覆盖目录按 `path_id` 聚合归档；Go 双向互通已验证 |
| 6.4 | 隧道 + fork 后台 | `comm.go:991-997`、`transfer.go:154-250` | ⚠️ CLI 已实现；库级后台未完成 | `trz`/`tsz` 已使用动态 loopback 端口、hello 握手和 Unix fork/setsid；`TrzszTransfer::background()` 仍是桩（Rust 当前定义见 `transfer.rs:254`），本目标不扩展库级 fork/background |
| 6.5 | 暂停/恢复 | `pipeline.go:340-406` | ✅ | V3+ `#DATA:=` heartbeat 与恢复已实现；CLI 首次 Ctrl+C 暂停、再次确认停止，filter 输入泵分流控制键；协议完整性有回归覆盖 |

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

当前核心 filter 回归不再依赖外部服务；Go 互通测试按需构建仓库 `trzsz-go` CLI。

| 层级 | 状态 / 后续 |
|---|---|
| 单测 | trigger、普通透传、close/error，以及 drag、OSC52、trace、ZMODEM、tmuxcc、Relay、Windows hook 的局部回归均可本地运行 |
| 协议层 | `tests/filter_interop.rs` 覆盖 Go `tsz`/`trz` 双向/目录传输；`tests/relay_interop.rs` 覆盖 Relay 跳板上传 |
| 真实互通 | 按需构建仓库 Go `cmd/tsz`、`cmd/trz`；Filter/Relay 与 Go V3/V4 互通不依赖固定 `/tmp/go-*` 二进制 |
| 库级 | Filter、Relay、WindowsConsoleGuard public API 有 integration/unit test；未新增 demo terminal host |

可选功能为注入式 Filter API；OSC52 剪贴板由 host callback 提供，ZMODEM 依赖 host 安装的 `sz`/`rz`。

---

## 核心库里程碑状态

| 里程碑 | 状态 | 证据 / 未包含项 |
|---|---|---|
| **M1 核心库可用** | ✅ | S/R/D、分片 trigger、普通输出透传、Go `tsz`/`trz` 本地互通与目录上传均由 `tests/filter_interop.rs` 覆盖 |
| **M2 核心体验** | ✅ | progress bar/observer、路径默认值、文件权限与资源收尾有回归覆盖 |
| **M3 宿主核心 API** | ✅ | callback、取消、terminal columns、picker selector 和 upload API 已公开；未新增 demo host，resize signal 仍由宿主接线 |
| **M4 Relay** | ✅（library API） | `TrzszRelay` lifecycle/handshake/tunnel/callback/close；真实 Go `trz` 跳板上传回归；`trzsz -r` CLI branch 未接线 |
| **M5 可选特性** | ✅（Filter API） | drag、ZMODEM、OSC52 callback、trace、tmuxcc、Windows console hook 均有本地回归；原生 GUI/系统剪贴板 UI 由宿主提供 |
| **M6 协议扩展** | ✅（协议/CLI 已有） | V2–V4、tunnel 与 Unix `-f` 已实现；库级 `TrzszTransfer::background()` 仍为桩，本目标未扩展 |

**后续边界**：不包含自动 SSH/PTY/CLI 生命周期、`trzsz -r` 命令接线、原生 GUI picker、库级后台传输以及 Windows runtime 验证；这些不影响注入式 Filter/Relay public API 的使用。
