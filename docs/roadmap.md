# trzsz-rs 实施路线（ROADMAP）

本文件是两份分析文档的总纲，回答"先做什么"：

- `docs/transfer-gap-vs-go.md` —— 文件传输链路差距分析（服务端 `trz`/`tsz`，含实测结论）
- `docs/library-porting-checklist.md` —— Go 库 API → Rust 移植清单（客户端 `TrzszFilter`/relay 等）

**一句话结论：先做传输文档的 P0 正确性批次，紧接着做库文档的 Phase 0（API 形态决策），然后全力推 Phase 1（数据泵）。** 不是二选一，而是明确的先后依赖。

---

## 一、为什么传输 P0 必须排在最前

两份文档的依赖关系是单向的 —— 库文档的"横切清单"本身就把它列成了 Phase 1-3 的前置：

| 传输 P0 项 | 阻塞库 Phase 1 的哪个环节 |
|---|---|
| `escape_chars` Latin-1 编解码（`trz.rs:206-209` + `escape.rs:115-127`） | rs filter ↔ rs `trz -b` 二进制模式直接失败 —— 客户端一跑就撞上 |
| `recv_check` `idx<1` + 脏行重同步 + `stripTmuxStatusLine` | **库最忌讳**：`panic = "abort"` 下脏输入会把宿主程序（tssh 这类）一起带走，不只是传输失败 |
| `send_file_data` EOF 校验（`transfer.rs:664-686`） | 无限循环会占死 filter 的接收线程（实测 4 秒 99030 个空 DATA 块） |
| `send_config` 发 `tmux_output_junk` | filter 侧 `mayHasJunk`（库 Phase 1.6）依赖对端发这个键才有意义 |
| `effective_directory()` 接进 `trz.rs`/`tsz.rs` | 独立缺陷，5 行改动，顺手修 |

三条判断依据：

1. **依赖方向**：传输正确性 → 库 API 形态 → 数据泵 → 体验 → 外围，单向不可逆。
2. **风险**：`panic = "abort"` + 脏输入 panic 是唯一会"伤到宿主程序"的问题，必须在库暴露给外部之前解决。
3. **可验证性**：第 1 步当天就能用现有测试/协议探针证明改对了；第 3 步之前任何工作都缺集成验证手段。

**工作量对比**也支持这个顺序：传输 P0 约 1-3 天、全是外科手术式改动、无设计决策；库 Phase 1 是并发结构重写，还要先过 Phase 0 的三个决策。

---

## 二、执行顺序

### 第 1 步 · 传输 P0（1-3 天，无设计风险）

对应 `docs/transfer-gap-vs-go.md` 第一节 1-5 与第五节 P0/P1：

1. `escape_chars` 改 Latin-1 单字节语义（发送侧 `trz.rs:206-209`、解析侧 `escape.rs:115-127`）—— 不修则 binary 模式不可用
2. `send_file_data` 补 EOF 校验（对齐 `transfer.go:890-898`）—— 不修则死循环
3. `recv_check` 加 `idx < 1` 保护 + panic→`serverError` 兜底 + `mayHasJunk` 重同步（`LastIndex("#TYPE:")`）+ `stripTmuxStatusLine`
4. `effective_directory()` 接进 `trz.rs:120,197,215` / `tsz.rs:80,196,206`
5. binary 降级提示后真正清 flag（`trz.rs:107-112`、`tsz.rs:104-109`）+ `send_config` 发 `tmux_output_junk`

**验收**：把 Python 协议探针固化成回归测试 —— 现有 4 个 interop 测试因缺 Go 工具链**全部静默跳过**，且只覆盖下载方向 + base64；需补上传方向、目录、binary、重名、空文件用例。

> ⚠️ 第 3 项与第 2 步的 `panic = "abort"` 去留要**一起决策**，别分两次改。

### 第 2 步 · 库 Phase 0（约半天，决策 + 收敛）

对应 `docs/library-porting-checklist.md` Phase 0：

- 定生命周期：`new()` 构造 → `run()` 数据泵 → `close()` 收尾（Go 是在 `NewTrzszFilter` 里隐式 `go wrapInput()/wrapOutput()`，`filter.go:121-122`）
- `pub` 边界收敛（现在内部类型全 `pub`，一发布就锁死重构）
- **去掉 `panic = "abort"`**（库要 unwind；与第 1 步③合并决策）
- crate 元数据（`repository`/`keywords`、crate-level `//!` 文档；不发 crates.io 就加 `publish = false`）
- 产出：目标 API 草图（清单文档里已有）

### 第 3 步 · 库 Phase 1.1-1.6 数据泵（最大价值块）

对应 `docs/library-porting-checklist.md` Phase 1：

- 1.1 `wrap_input` / 1.2 `wrap_output` / 1.3 `handle_trzsz` / 1.4 `download_files` / 1.5 `upload_files` —— 先跑通 **base64 模式最小闭环**
- 1.6 `detect_trzsz` 加固（`LastIndex`、**`TRZSZ→TRZSZGO` 重写**、重复 uniqueID 去重、`win_server` 判据修正、tmuxcc prefix）—— 缺重写会导致嵌套过滤器重复触发，应与 1.1-1.5 同批

**验收**：rs filter ↔ 真实 Go `trz`/`tsz` **双向**互通；CI 加 Go 工具链 job 构建 `/tmp/go-trzsz`、`/tmp/go-trz`、`/tmp/go-tsz`，让 interop 真正跑起来。

> 这一步做完，`trzsz` 二进制才第一次"真的能用" —— 这是当前最大的功能空洞。

### 第 4 步 · 库 Phase 2 + 传输 P1

- `on_step` 接进 `send/recv_file_data`、`create_progress_bar`、文件 `close()`（进度否则永远 0→100 跳变）
- 传输 P1：`perm|0600`/`perm|0700` 权限保留、`delete_created_files` 兼容普通文件、`-r` 收尾

### 第 5 步 · 库 Phase 3（宿主接入）

回调 `set_transfer_state_callback` / `set_redraw_screen_func`、Ctrl+C → `stop_transferring_files`（需给 `TrzszBuffer` 加 stop channel）、SIGWINCH → `set_terminal_columns`、默认上传/下载路径 → 对话框（**必须给弹窗加开关**，嵌入方未必想弹）。

**到这里才算能对标 trzsz-go 使用。**

### 第 6 步 · 可并行分支（按需排期）

- **库 Phase 4**：relay（`trzsz -r` 跳板）
- **库 Phase 5 / 传输外围**：drag、zmodem、OSC52、trace log、tmuxcc、Windows VT
- **传输 P2 = 库 Phase 6**：协议 V2 流水线 + zstd（让 `-c` 生效）→ V3 断点续传 → V4 archive → 隧道/fork

---

## 三、两个"不要先做"

| 不要先做 | 原因 |
|---|---|
| **协议 V2+/zstd（传输 P2 = 库 Phase 6）** | 工作量最大（`pipeline.go` 1076 行）但杠杆最低：基础互通还没稳，压缩与流水线的吞吐收益用户感知不到 |
| **Phase 5（drag / zmodem / OSC52）** | 开关虽在但属外围功能，且 Phase 1 没做完它们连挂载点都没有 |

---

## 四、里程碑与出口判据

| 里程碑 | 覆盖步骤 | 出口判据 |
|---|---|---|
| **M0 服务端正确** | 第 1 步 | 传输 P0 全部有回归测试；`trz -b` 与 Go 客户端可互通；脏输入不崩 |
| **M1 库可用** | 第 2-3 步 | rs filter 与 Go `trz`/`tsz` 双向真实互通；`trzsz` 二进制能完成一次传输 |
| **M2 体验对齐** | 第 4 步 | 进度条实时、`-r` 生效、权限保留、binary 可用 |
| **M3 宿主接入** | 第 5 步 | demo 宿主完成"回调 / 取消 / resize"三件事 |
| **M4+** | 第 6 步 | relay 走通跳板；`-c` 生效、断点续传 |

**关键路径**：`第1步 → Phase 0 → Phase 1.1-1.6 → Phase 2.1 → Phase 3.3`。
