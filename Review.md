# 2026-10-20 对抗性 Bug 审查与修复报告

## 审查范围和基线

- 仓库：`netstack-smoltcp` 0.3.1；基线为 `main` 分支的 `0570935`。
- 检查范围：TCP/UDP、IP 分发、有限容量队列、虚拟设备、异步 Sink/Stream 契约及现有测试。
- 初始门禁：`cargo test --all-targets` 通过（仅 1 个测试）；`cargo clippy --all-targets -- -D warnings` 通过；`cargo fmt --all -- --check` 存在一处既有格式差异。
- 修复流程：每项先添加确定性失败测试并观察失败，再实施最小修复并观察测试转绿。

## 已确认问题与 QA 复现

以下测试命令均在项目根目录执行；每个新增测试在修复前失败、修复后通过。

| 编号 | 等级 | 位置 | 根因与影响 | 回归命令 |
| --- | --- | --- | --- | --- |
| B01 | P2 | `src/udp.rs` | 空 UDP 负载直接返回成功，没有生成合法的仅含包头的数据报，导致保活包或空消息丢失。 | `cargo test --test regression udp_zero_length_datagram_is_forwarded` |
| B02 | P1 | `src/udp.rs` | 无效 UDP 包令 `poll_next` 返回 `Ready(None)`，错误终止接收流，阻断后续合法数据。 | `cargo test --test regression malformed_udp_datagram_does_not_terminate_receive_stream` |
| B03 | P1 | `src/device.rs` | 先出队入站包、再申请出站容量，满队列时造成不可恢复的入站丢包。 | `cargo test --lib full_egress_queue_does_not_discard_ingress_packet` |
| B04 | P2 | `src/stack.rs` | `Sink::poll_close` 不刷新缓冲包就关闭，最后一帧会被丢弃。 | `cargo test --test regression closing_stack_sink_flushes_buffered_packet` |
| B05 | P2 | `src/stack.rs` | 配置零容量队列时 Tokio 内部 panic，违反 `build() -> io::Result` 的错误处理预期。 | `cargo test --test regression zero_channel_capacity_returns_an_error_instead_of_panicking` |
| B06 | P1 | `src/tcp.rs` | TCP 写半关闭等待整个连接结束，而非本地 FIN 发起；进入 `Closing` 时也没有唤醒 shutdown 任务。 | `cargo test --lib shutdown_completes_after_local_fin_is_requested` |
| B07 | P1 | `src/tcp.rs` | 重传 SYN 被当成新连接，为相同四元组重复分配 `TcpStream` 与 socket。 | `cargo test --test regression retransmitted_tcp_syn_does_not_create_duplicate_stream` |

### 手动 QA 步骤

1. **空 UDP 包**：创建仅启用 UDP 的协议栈，向有效 IPv4 端点发送空负载；确认出站有 20 字节 IPv4 头和 8 字节 UDP 头。
2. **畸形 UDP 恢复**：依次注入 UDP 段仅 3 字节的 IPv4 包和一个合法 UDP 包；接收流必须继续返回第二包的负载，不能返回 EOF。
3. **出站背压**：填满容量为 1 的虚拟设备出站队列，注入一帧入站包并尝试接收；释放出站容量后，原入站包仍须可读。
4. **Sink 关闭**：用 `feed` 缓冲一个有效 UDP/IP 包，不显式 `flush`，直接调用 `close`；UDP 接收端仍须获得该帧。
5. **非法容量**：分别将已启用的 stack、UDP、TCP 队列容量设为零；`build` 应返回 `InvalidInput`，不得 panic。
6. **TCP 半关闭**：触发写端 shutdown，将发送缓冲数据移交 smoltcp 并进入 `Closing`；等待任务应被唤醒并完成，而不是等待对端 FIN。
7. **重复 SYN**：启动 TCP runner/listener，连续注入两次相同 IPv4 SYN；只能接收一个 stream。另需专项验证 socket 销毁后四元组可复用。

## 代码变更与实测

- `src/udp.rs`：允许零负载数据报发送；忽略畸形帧并继续从 UDP 队列读取。
- `src/device.rs`：先预留出站容量，再取出入站包。
- `src/stack.rs`：校验已启用队列容量；Sink 关闭前先 flush。
- `src/tcp.rs`：发起本地 FIN 后完成 shutdown 并唤醒等待任务；按四元组去重，socket 移除时清除记录。
- `tests/regression.rs` 和模块单元测试：新增 7 项确定性回归，保留原有 1 项。

修复后：`cargo test --all-targets` **8/8 通过**；`cargo clippy --all-targets -- -D warnings` **通过**；`cargo fmt --all -- --check` **通过**。

## 范围限制与后续专项

- 3 个只读子审查任务的远程 `read_file` 调用长时间挂起，未能形成独立源码报告。以上“确认”均基于主审查实际读取的源码和亲自观察的红绿测试，不把子代理的推测算作独立验证。
- 本轮未完成真实 TUN E2E 回放、模糊测试、跨平台实机测试、长时间负载测试，也未覆盖 IPv4 分片和 IPv6 扩展头处理。
- TCP shutdown 的单元测试验证了 `Closing` 状态契约，仍需与真实对端、拥塞和双向 FIN 交互做端到端测试。
- SYN 测试验证了去重，但 socket 关闭后的四元组复用仍需独立集成测试。
- 提交及 PR 状态以 Git 历史和 GitHub 记录为准；本文保留测试事实与尚未覆盖的风险。
