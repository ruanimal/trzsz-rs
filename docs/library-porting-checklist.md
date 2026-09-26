# trzsz-rs 库化路线：Go 库 API → Rust 目标 API 移植清单

> 实施顺序与里程碑见 [`docs/roadmap.md`](roadmap.md)；服务端传输差距见 [`docs/transfer-gap-vs-go.md`](transfer-gap-vs-go.md)。

**背景**：`trzsz-go` 不只是 CLI，还是被真实项目依赖的 Go 库（pkg.go.dev "Imported by" 3 个模块 / 6 个包：`trzsz/trzsz-ssh/tssh`、`abakum/{cssh,dssh,trzsz-ssh}/tssh`、`jixishi/SerialTerminalForWindowsTerminal`、`shoaibashk/nanocom`），核心价值是 `TrzszFilter` —— 让宿主终端程序在本地侧具备触发上传/下载的能力。
`trzsz-rs` 结构上同样是 lib + bin；客户端 `TrzszFilter` 的 Phase 0–3 核心已实现，Go 的可选 filter 功能、Relay 与自动 CLI/PTY 包装仍不在本次范围。

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

当前核心 filter 回归不再依赖外部服务；Go 互通测试按需构建仓库 `trzsz-go` CLI。

| 层级 | 状态 / 后续 |
|---|---|
| 单测 | 已覆盖 fragmented/malformed trigger、普通输入输出透传、close 和 I/O error；可继续对齐 Go detector 全量边界用例 |
| 协议层 | `tests/filter_interop.rs` 本地 Go `tsz`/`trz` 覆盖 filter 下载、单文件/目录上传和 picker 取消 |
| 真实互通 | 测试直接构建仓库 Go `cmd/tsz`、`cmd/trz`，不依赖固定 `/tmp/go-*` 二进制 |
| 库级 | public API 通过 integration test 调用；未新增 demo terminal host |

可选 OSC52/drag/zmodem/trace/tmuxcc 与 relay 不属于本次核心 filter 范围。

---

## 核心库里程碑状态

| 里程碑 | 状态 | 证据 / 未包含项 |
|---|---|---|
| **M1 核心库可用** | ✅ | S/R/D、分片 trigger、普通输出透传、Go `tsz`/`trz` 本地互通与目录上传均由 `tests/filter_interop.rs` 覆盖 |
| **M2 核心体验** | ✅ | progress bar/observer、路径默认值、文件权限与资源收尾有回归覆盖 |
| **M3 宿主核心 API** | ✅ | callback、取消、terminal columns、picker selector 和 upload API 已公开；未新增 demo host，resize signal 仍由宿主接线 |
| **M4 Relay** | ⏭ | 明确不属于 `TrzszFilter` 核心范围 |
| **M5 可选特性** | ⏭ | drag、ZMODEM、OSC52、trace、tmuxcc 与原生 GUI picker 未实现 |
| **M6 协议扩展** | ✅（服务端已有） | Rust V2–V4 transfer 实现先前已存在，本次未扩展线缆协议 |

**后续边界**：可选 Phase 5、Relay、自动 signal/PTY/CLI 集成仍未实现，不影响注入式 `TrzszFilter` 核心 API 使用。
