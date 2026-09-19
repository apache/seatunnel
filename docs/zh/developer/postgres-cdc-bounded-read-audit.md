# PostgreSQL CDC 有界读取审计

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

## 结论与范围

**实现前需要达成设计共识。本文不启用有界 WAL 读取。**
这是 [#11739](https://github.com/apache/seatunnel/issues/11739) 的 PostgreSQL 部分，
不表示已经审计其他连接器。PostgreSQL 的 `stop.mode` 默认值和唯一支持值仍为 `never`。
已有的 `startup.mode = snapshot-only` 是独立能力，不代表增量读取支持停止边界。

源码基线为 `b37af3a9bf634735cabe4908014c2d390b288558`，Debezium 版本为 `1.9.8.Final`。
2026 年 9 月 16 日重新阅读了 issue 正文及全部两条评论。后续讨论要求先确认偏移量排序、
边界执行位置和真正的任务结束证据，再按连接器提交实现。该讨论中没有 PostgreSQL stop-mode
认领记录。针对 `11739` 和 `"stop.mode" postgres` 的开放 issue/PR 搜索没有发现单独的
PostgreSQL stop-mode 实现；这只是当日搜索结果，不代表保留工作所有权或排除未公开工作。

## 重叠工作

davidzollo 的 [PR #11556](https://github.com/apache/seatunnel/pull/11556) 在审计时仍开放，
head 为 `ba7c8bd743c8a97cd308bf322e3cbde58b58c552`。它负责快照期间的有界 WAL 回填，
包括 `PostgresWalFetchTask.PostgresWalSplitReadTask`、快照 reader 的复制槽隔离、
枚举器创建流式复制槽和水位线采集。其包装类设置
`PostgresOffsetContext.streamingStoppingLsn`，调用现有 Debezium 流式读取器，
检查 producer 异常后通过现有 dispatcher 发送 END。

应在评审和集成后复用该架构，不应复制一份独立实现。快照数据合并和面向用户的增量终止，
即使最终共用 reader，也需要不同的边界策略。该 head 的外层增量 `execute()` 仍创建
无界流式读取器，因此 #11556 本身没有实现 `stop.mode`。

9 月 12 日的评审仍要求同步 dev、转义分片表名过滤表达式、在合并 Debezium 属性后校验
实际生效的复制槽名称，并添加测试。此前讨论还指出清理行为文档不符及 reader 崩溃后
遗留回填槽的问题。这些是开放 PR 的评审发现，不是本次审计修复或独立复现的数据库事故。

[PR #11029](https://github.com/apache/seatunnel/pull/11029) 也在调整 PostgreSQL/OpenGauss
源码归属及选项隔离。需要协调最终入口；目前不能仅修改共享选项对象就安全地只为
PostgreSQL 启用新模式。

## 按契约评估可行性

### 配置：需要 PostgreSQL 专用契约

`PostgresSourceOptions.STOP_MODE` 仅声明 `NEVER`，两个连接器工厂都使用它。
`StopConfig.getStopOffset()` 将 `SPECIFIC` 路由到 `OffsetFactory.specific(String, Long)`，
而 `LsnOffsetFactory` 明确拒绝这个重载。其 map 重载不在 `StopConfig` 的调用路径上。
仅开放 `SPECIFIC` 会把错误推迟到运行阶段，不会产生有界读取能力。

新增选项前需确定 LSN 表示及验证：PostgreSQL `X/Y` 文本、无符号排序、非法值和保留的
never-stop 哨兵值拒绝规则、终点早于起点时的行为。不能静默改变 MySQL 文件名/位置契约，
也不能使用有符号 long 比较。暂不开放 `latest` 或 timestamp 停止模式。
`latest()` 采集的是 WAL 位置，不是已证明的解码事务边界，采集阶段及持久化也需单独定义。

### 偏移量：可比较事件顺序，不代表事务已完成

`LsnOffset.compareTo()` 比较 `lsn`，并特殊处理 never-stop。`getLsnCommit()` 读取
`lsn_commit`，缺失时回退到事件 LSN。相同事件 LSN 可以对应不同的已提交 LSN。
例如 `lsn=200, lsn_commit=100` 会被判断为超过终点 150，但提交位置尚未到达 150。
配套测试描述这个区别，不表示观测到了真实生产事务轨迹。

Debezium 的 `PostgresStreamingChangeEventSource.processMessages()` 使用
`lastCompletelyProcessedLsn` 判断停止条件。它在事务/DML 分支之前，依据
`message.isLastEventForLsn()` 更新该值。`PgOutputReplicationMessage` 对普通 DML
也返回 true。因此完成一个事件 LSN 不能证明已经处理 COMMIT。
[逻辑复制协议](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
也区分提交 LSN 与事务结束 LSN。

**建议策略，尚未获得设计认可：** 包含提交边界不晚于终点的完整事务，排除晚于终点的
完整事务，不允许只发出跨界事务的一部分。需明确终点表示 commit record LSN 还是
transaction end LSN，以及各解码插件如何在输出行之前暴露它。若需要事务缓冲或新增
decoder hook，应单独设计，不能在本次工作中加入无界缓冲或复制 Debezium 私有循环。
向上取整并包含跨界事务会产生超出终点的数据，是另一种策略，需要明确批准。

### 空闲 WAL：缺少可靠的进度观测

Debezium 空轮询时从现有 offset 状态发出 heartbeat，不会推进
`lastCompletelyProcessedLsn`。WAL 可以到达某位置而没有相应的选中表事件。
目标之后的新消息也可能在下一次循环条件检查前就被发送。这两种行为都不能建立严格的
面向用户的停止契约。

#11556 从 `PostgresUtils.currentLsn()` 中移除 `currentTransactionId()`，避免采集水位线
时额外产生 WAL。这仅消除空边界的一种来源，不证明任意用户 LSN 可解码或空闲任务可结束。
设置停止 LSN 还会启用 Debezium 的快照前追赶事务处理行为，应用到普通增量任务前也需
验证其生命周期。

必须证明选中表空闲时 decoder 仍已处理到边界。超时、空队列、服务器 WAL 末端，或其他
消费者的槽确认位置，都不能单独作为证据。若使用标记写入，需要明确源端副作用、权限
和插件兼容性，必须先讨论设计，不能隐藏写入。

### END、队列排空与 FINISHED：完成信号与尚未证明的排空保障

对于已分配的有限分片，`IncrementalSourceStreamFetcher.isBoundedReadFinished()` 要求
`taskStarted`、`executing == false`、`streamFetchTask.isRunning() == false`，以及非 null、
非 never-stop 的停止 offset。`pollSplitRecords()` 在该条件成立且记录迭代器没有下一项时
返回 null。`IncrementalSourceReader.onSplitFinished()` 处理增量分片完成。END 不是
stream fetcher 的完成条件：MySQL 有界 producer 会发送 END，而
`IncrementalSourceScanFetcher` 消费它以结束快照回填。不能把该 producer 约定当作
stream reader 的必要条件。

允许轮询的条件与稍后的完成检查是分开的观测，不构成原子的最终排空。源码分析提示一种
可能的交错：producer 仍在运行时 `queue.poll()` 返回空，随后 producer 在较晚的完成检查
之前将最后一批数据入队并结束。fetcher 此时可能在队列仍有记录时返回 null。这是
**源码层面的推断，并非已经复现的故障**；场景 5 必须验证该窗口，不能假定已有队列排空保障。

producer 必须证明成功到达边界，并使 `isRunning() == false`。目前 `PostgresWalFetchTask`
在入口将 `taskRunning` 置为 true，仅在 `shutdown()` 中清除。直接替换为 #11556 的包装类
会使该标志仍为 true。producer 异常和取消不能被报告为成功的有界完成。委托返回后的
`context.isRunning()` 本身也不证明到达终点：Debezium 在禁用 streaming 时可以提前返回。
需保留异常传播，并证明最后符合条件的行已交付、分片耗尽及 Zeta FINISHED 的顺序。
不能为了补偿无法证明完成的 PostgreSQL producer 而改写共享 fetcher。

### Checkpoint 与复制槽：需要最终生命周期证据

`IncrementalSplit` 保存起点和终点，恢复时必须保留原始终点。
`PostgresSourceFetchTaskContext.loadStartingOffsetState()` 恢复事件、提交及完整处理位置。
`PostgresWalFetchTask.commitCurrentOffset()` 向 Debezium 传递提交 LSN。
Debezium 在 execute 返回时关闭复制连接，之后如果 stream 不存在会忽略 offset 提交。
必须结合最终 checkpoint 验证该顺序；reader 已停止不代表槽确认或 sink 提交已持久化。

保留默认的主复制槽保留策略。在声称支持 drop-on-stop 前，区分成功、正常取消、重启
及进程死亡。在恢复状态持久化之前删除槽可能破坏重放。#11556 的回填槽属于独立的
资源归属问题，应继续在该工作中解决。Java `finally` 清理不等于服务端临时槽保障，
也不得清理其他任务的槽。

### OpenGauss：共享实现不等于独立验证

`OpengaussIncrementalSourceFactory` 构造 `PostgresIncrementalSource` 并复用 STOP_MODE，
OpenGauss 模块则提供自己的 PostgreSQL 连接及复制连接类。改变共享选项也会为它开放
新模式。除非分别证明 decoder、空闲进度、槽生命周期和恢复行为，否则 OpenGauss
应保持 `never`。与 #11029 协调选项隔离，不能把 PostgreSQL E2E 当作 OpenGauss 证据。

## 后续实现的验收场景

以下是必要场景，**不是本次审计已经提供或通过的运行时测试**。扩展现有 `PostgresCDCIT`
及 CDC base 测试，不创建另一套独立容器测试。可复用 MySQL 有界读取的完成验证方式，
不能复用其 binlog 排序假设。

1. **配置：** 默认和显式 `never` 保持行为。拒绝非法 LSN、never-stop 哨兵、不支持的
   插件/模式组合及终点早于起点，并提供清晰错误。证明只为 PostgreSQL 开放时不影响 OpenGauss。
2. **事务边界：** 写入前建立复制槽，生成多行事务 T1、T2、T3。采集所选 decoder 的实际
   commit/end 位置，不能仅用稍后的 `pg_current_wal_lsn()` 代替。终点在约定的 T2 边界时，
   必须包含全部 T1/T2，排除 T3。终点在 T2 内部时，按获批策略完整包含或排除 T2，不能
   只包含前缀。覆盖共享 LSN 的多条消息和 long 符号边界排序。
3. **空闲边界：** 覆盖没有选中表记录、仅有被过滤表写入、目标没有可解码记录及尚未到达
   的未来终点。已到达的空闲终点无需测试注入“救援写入”即可结束；未来终点不能提前结束。
   对声明支持的 `pgoutput`、`decoderbufs` 分别验证，并覆盖事务元数据开/关。
4. **快照交互：** 在 exactly-once initial 快照期间并发 DML，终点分别位于水位线范围前、
   内、后。不兼容组合应被拒绝，不能在严格历史截断语义下静默返回较新的快照数据。
   必须具备 #11556 的回填和过滤测试，包括含正则元字符的合法表名。
5. **队列排空：** 使用确定性 gate 阻塞 sink，产生超过一批队列数据后到达终点。释放 gate，
   验证所有符合条件的行、没有越界行、分片耗尽和真实 FINISHED，不要求 END 作为 stream
   完成信号。另用 gate 控制最后一次入队发生在空队列轮询之后，且 producer 在较晚的完成
   检查之前结束；要求最后一批数据在分片耗尽之前交付。该场景针对上述源码推断、尚未复现
   的窗口。过滤后空输出不算失败；producer 异常、取消或 streaming 禁用不能算作成功到达边界。
6. **恢复：** 在事务中途、消费到终点但尚未排空队列时 checkpoint；使用同一终点恢复，
   另测最终 checkpoint 后的 failover。验证约定的重放保障、精确 sink 状态、不会重新开启
   无界流，且无需新写入即可完成。覆盖缺少 `lsn_commit` 的旧 offset，以及带/不带 `txId`
   的快照水位线；仅能反序列化不代表恢复正确。
7. **复制槽：** 观测停止前及最终 checkpoint/关闭后的槽存在性、活跃 owner 和确认 LSN。
   测试默认保留、显式删除策略、正常完成、正常取消和 worker 突然死亡。验证承诺的重启
   能力，不删除其他任务槽。回填槽清理在 #11556 下单独验证。
8. **端到端状态：** 在有界超时内轮询 Zeta 到 FINISHED，等待所有 job future 并验证退出码
   和精确 sink 数据，不能通过取消任务让断言通过。失败清理放在 `finally`，使用动态端口
   和条件 gate，超时记录最后状态、offset 和槽状态。PostgreSQL 成功不代表 OpenGauss、
   其他引擎或整个 CI workflow 成功。

## 复现与证据边界

`PostgresSourceConfigFactoryTest` 新增的行为描述测试使用实际 PostgreSQL STOP_MODE
选项调用 `ConfigValidator`：省略选项或 `never` 验证成功，`specific`、`latest` 和
`timestamp` 验证失败。这是独立选项契约复现，不是完整任务提交。
`LsnOffsetTest` 增加事件/提交位置区分、相同事件 LSN 和无符号排序场景。
这些测试应在未修改的生产基线上通过；后续实现只能替换真正获得验证的模式的拒绝断言。

在仓库根目录使用支持的 Java 工具链执行：

```shell
./mvnw -pl seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres \
  -Dtest=PostgresSourceConfigFactoryTest,LsnOffsetTest test
```

本次工作没有生产代码、新停止选项、E2E 成功声明、发布证据或事故证明，也没有导入
#11556、解决其评审问题或关闭整个 umbrella issue。后续实现需要边界与空闲进度设计
共识、重叠 reader 的集成或经同意的提取、OpenGauss 隔离，以及上述验收覆盖。

## 源码索引

- PostgreSQL 模块：`source/PostgresSourceOptions`、`source/offset/LsnOffset`、
  `source/offset/LsnOffsetFactory`、`source/reader/wal/PostgresWalFetchTask`、
  `source/reader/snapshot/PostgresSnapshotFetchTask`、`source/reader/PostgresSourceFetchTaskContext`。
- CDC base 模块：`config/StopConfig`、`source/enumerator/IncrementalSplitAssigner`、
  `source/split/IncrementalSplit`、`source/reader/external/IncrementalSourceStreamFetcher`、
  `source/reader/IncrementalSourceReader`。
- Debezium [v1.9.8.Final 流式读取器](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresStreamingChangeEventSource.java)
  和 [pgoutput 消息](https://github.com/debezium/debezium/blob/v1.9.8.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputReplicationMessage.java)。
