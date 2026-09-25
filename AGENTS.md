# AGENTS.md

## 项目
Rust 2024 实现的 trzsz，包含 `trz`、`tsz` 和 `trzsz` 命令；传输协议需与 Go 实现兼容。

## 代码位置
- `src/args.rs`：命令行参数；`src/trz.rs`、`src/tsz.rs`：上传/下载入口。
- `src/transfer.rs`：协议与文件传输；`src/buffer.rs`：输入缓冲；`src/escape.rs`：消息编码及 binary 转义。
- `src/comm.rs`：文件/路径、公共协议类型及系统辅助；`src/filter.rs`、`src/trzsz.rs`：终端过滤器与包装入口。
- `tests/`：协议、端到端及互通测试。

## 编码与修改约定
- 遵循现有 Rust 风格；所有 Rust 改动必须经 `cargo fmt` 格式化，并通过 `cargo fmt --check`。
- 保持线缆格式和 Go 互通行为兼容；错误输入返回明确错误，不要在传输路径上 panic。不要对协议输入使用未经校验的索引、`unwrap` 或 `expect`。
- 新增或修改行为时，添加可本地运行的回归测试，覆盖正常路径、相关错误输入和边界情况；传输测试不得依赖外部服务。
- 提交前运行 `cargo fmt --check`、`cargo test` 和 `cargo build --locked`；确实无法运行时说明原因及未验证项。
- 按请求限制改动范围，避免无关重构；新增依赖需有必要理由并更新 `Cargo.lock`。保留 release `panic = "abort"` 设置。

## 验证
```sh
cargo fmt --check
cargo test
cargo build --locked
```

真实 Go 互通测试可能依赖 `/tmp/go-trzsz`、`/tmp/go-trz`、`/tmp/go-tsz` 等外部二进制；缺少时不能据此宣称真实互通已验证。
