# trzsz-rs 实施路线（ROADMAP）

本文件是两份分析文档的总纲，回答"先做什么"：

- `docs/transfer-gap-vs-go.md` —— 文件传输链路差距分析（服务端 `trz`/`tsz`，含实测结论）
- `docs/library-porting-checklist.md` —— Go 库 API → Rust 移植清单（客户端 `TrzszFilter`/relay 等）

**当前进度**：传输 P0/P1 与 V2–V4 协议已完成；库侧注入式 `TrzszFilter` 核心 Phase 0–3 已实现并通过本地 Go CLI 双向/目录互通。Relay 与 Phase 5 可选特性仍未实现，详见 [`docs/library-porting-checklist.md`](library-porting-checklist.md)。

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

**当前验证**：`tests/filter_interop.rs` 构建仓库 Go `tsz`/`trz`，覆盖 Go→Rust filter 下载、Rust filter→Go 文件/目录上传及选择取消；外部 Go 工具缺失时该测试会明确失败。

> ⚠️ 第 3 项与第 2 步的 `panic = "abort"` 去留要**一起决策**，别分两次改。

### 第 2 步 · 库 Phase 0（API 形态决策）✅

已收敛为 `new()` 构造 → `run()` 阻塞运行 → `close()` 收尾；端点仍由宿主注入，filter 内部状态私有。保留 release `panic = "abort"`，对协议输入做校验；crate 发布元数据不属于本次目标。

### 第 3 步 · 库 Phase 1 数据泵与传输闭环 ✅

`TrzszFilter` 已实现 client/server 双向 pump、跨 read trigger 检测、S/R/D worker、host path selectors、主动/一次性上传和关闭/错误收尾。`tests/filter_interop.rs` 构建仓库 Go `tsz`/`trz`，实测 Go→Rust 下载及 Rust→Go 文件/目录上传。

### 第 4 步 · 库 Phase 2 进度与资源管理 ✅

filter 按 CFG quiet 创建进度条，逐 chunk 更新宿主 progress callback，并在成功/取消/失败路径恢复光标；transfer 层文件 close、EOF 校验和权限等此前已完成。

### 第 5 步 · 库 Phase 3 宿主联动 ✅（核心 API）

已提供状态/重绘 callback、取消/删除、终端列更新、默认路径与可注入路径选择器。blocking reader 可注册 shutdown handler。宿主负责接入 resize signal；本次不提供原生 GUI picker 或 demo host。

### 后续步骤 · 明确排除

Relay（Phase 4）、drag/ZMODEM/OSC52/trace/tmux control mode（Phase 5）和自动 SSH/PTY/CLI 包装仍未实现；它们不属于本次 `TrzszFilter` 核心闭环。Rust transfer 的 V1–V4 协议早已支持，本次不改线缆格式。

### 第 6 步 · 可并行分支（按需排期）

- **库 Phase 4**：relay（`trzsz -r` 跳板）
- **库 Phase 5 / 传输外围**：drag、zmodem、OSC52、trace log、tmuxcc、Windows VT
- **传输 P2 = 库 Phase 6**：协议 V2 流水线 + zstd（让 `-c` 生效）→ V3 断点续传 → V4 archive → 隧道/fork

---

## 三、当前明确未纳入的扩展

| 范围 | 状态 |
|---|---|
| Relay / `trzsz -r` 跳板 | 未实现，本次 filter 只处理注入的四个 I/O 端点 |
| drag、ZMODEM、OSC52、trace、tmux control mode、原生 GUI picker | 可选特性，未实现 |
| 自动 SSH/PTY/CLI 生命周期与 resize signal 注册 | 由宿主负责；filter 提供 `run`/`close`、shutdown handler 和 terminal-columns API |
| transfer protocol V2–V4 | 既有 Rust transfer 层已实现；本次不改线缆协议 |

---

## 四、里程碑与出口判据

| 里程碑 | 状态 | 证据 / 边界 |
|---|---|---|
| **M0 服务端正确** | ✅ | 传输 P0/P1 回归通过，Go V4 transfer interop 在 `tests/v3_go_interop.rs` |
| **M1 核心 filter 可用** | ✅ | `tests/filter_interop.rs` 覆盖 S/R/D、本地 Go 互通、目录与取消 |
| **M2 核心体验** | ✅ | progress callback/bar、错误/EOF/close 清理与 terminal columns API |
| **M3 宿主核心 API** | ✅ | paths/selectors、上传入口、状态/重绘回调和 stop API；未新增 demo host |
| **M4 Relay** | ⏭ | 不属于已批准的 TrzszFilter 核心目标 |
| **M5 可选特性** | ⏭ | 按需排期 |
| **M6 协议扩展** | ✅（既有） | V2–V4 已存在；本次不触碰协议能力 |

**后续工作**：如需对齐 Go 的 Relay、可选 UI 功能或 wrapper 集成，应作为独立任务排期。

**关键路径**：`第1步 → Phase 0 → Phase 1.1-1.6 → Phase 2.1 → Phase 3.3`。
