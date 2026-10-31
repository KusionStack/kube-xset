# ConcurrentPatches 与先扩后缩替换

## 用户语义

ConcurrentPatches 控制每个逻辑副本的目标 Patch 组合；Replace 控制该副本如何更新。
启用 Replace 后仍先创建新 Instance，等待其目标版本 Available，再删除旧 Instance。
本变更不增加 CRD 字段，不修改传统模式的发布范围计算。

替换期间两个物理 Instance 属于一个逻辑副本：

- ByPartition 的分母不因替换临时扩容而变化，包括复用较小空闲 ID 的情况。
- 新旧 Instance 使用相同的目标 Patch 组合，但各自保留真实的 applied 状态供 Inspect 查询。
- 替换不是业务扩容，不重新触发 NewInstancePolicy；真正扩容仍使用该 Policy。
- ByLabel 命令可发送到任一替换成员；相同命令合并，矛盾命令拒绝本轮调和，不猜测先后顺序。
- 推进、暂停、回滚某个 Patch 不得推进另一个 Patch，也不得提前删除旧 Instance。

## 实现约束

1. 通过持久化 ResourceContext 的替换关系，在调用通用 Patch planner 前收敛为逻辑副本。
2. 替换关系同时存在时使用原 ID 作为逻辑 ID；关系清理后新成员独立接管，不依赖进程内缓存。
3. 有真实新成员时以其 Patch 组合保留正在执行的意图；尚未创建时使用原成员，不能把 placeholder 当作额外副本。
4. planner 只运行一次，再把目标组合投影回两个物理 ID。命令快照仅包含各自实际观察到的命令，防止清理另一个对象的命令。
   若命令发给新成员并触发其重建，沿用原成员上 Replace 已持久化的目标 Patch ID 组合，不能因新成员及其命令消失而丢失意图。
   替换标记与执行意图通过同一次原子 Patch 写入，之后才删除过时的新成员。执行意图使用独立内部 annotation，不读取 Inspect 的观测快照，且不依赖中间 ControllerRevision 一直存在。
5. Patch 状态按逻辑 ID 去重。任一仍存在的成员携带 Patch，都算仍在使用；撤销 Patch 必须等旧成员消失后才能回收日志。
   替换对的 Ready/Available 只认可已达到完整目标组合的成员，不能把旧成员上的 P1 与过时新成员上的 P2 拼成“全部可用”，否则会提前通过 compaction barrier。
6. 新成员版本必须与原成员当前目标完全一致、未进入删除流程且 Available，才能完成替换。
7. 自动 compaction 使用 `xset.kusionstack.io/compact-revision-plan` 标记本次操作的旧 resourceVersion。支持用户删除 Patch 的 admission 只有在版本匹配、Head 未变、请求恰好等于可信旧 status 可回收的日志时，才将其解释为 compaction；普通删除或过期标记仍执行 withdrawal。标记保留可支持 IfNeeded，但不能匹配下一次更新。

## 验证

- UT：高/低新 ID、多个替换对、原成员消失、placeholder、重启恢复、NewInstancePolicy、ByLabel 命令冲突。
- UT：Selected/Updated/Ready/Available 和 pending commands 去重，withdrawal 等待旧成员退出。
- UT：错误版本的新成员即使 Available 也不能触发旧成员删除。
- ASE 集成：真实 admission + Deployment/Set controller，覆盖替换中暂停、独立推进、回滚和 ByLabel 命令。

测试模拟 Runtime 的 Ready/Available 信号，不宣称覆盖实际 Runtime 的流量切换或运行时更新。
