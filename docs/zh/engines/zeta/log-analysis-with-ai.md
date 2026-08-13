---
sidebar_position: 15
---

# 使用 AI 辅助诊断运行日志

AI 工具可以帮助总结 SeaTunnel 日志，并找出值得进一步排查的证据。AI 的输出并不是已经确认的根因。对于建议修改的配置项和修复方法，都需要通过 SeaTunnel 文档、连接器文档和实际部署环境进行验证。

本文主要介绍 SeaTunnel Engine（Zeta）。如果作业运行在 Flink 或 Spark 上，需要同时收集对应引擎的 Driver 和 Worker 日志以及 SeaTunnel Starter 日志。下文介绍的 Zeta REST 日志接口不会收集 Flink 或 Spark 的运行日志。

:::caution 保护生产数据
日志和作业配置可能包含凭据、连接地址、SQL、数据记录、内部主机名等敏感信息。只使用组织允许的 AI 服务。上传前必须脱敏，不要上传私钥、访问令牌、堆转储或完整的生产配置文件。
:::

## 诊断流程

不要直接把整个日志文件发送给 AI 工具。建议按以下步骤处理：

1. 记录运行环境信息。
2. 从所有相关节点收集目标作业的日志。
3. 保留第一次失败和完整的异常链。
4. 删除无关日志并对敏感值进行脱敏。
5. 要求 AI 分别输出证据、假设和验证步骤。
6. 使用 SeaTunnel 文档、指标和外部系统验证分析结果。

### 记录运行环境

在条件允许时，记录以下信息：

- SeaTunnel 版本
- 执行引擎和部署模式
- 批处理或流处理模式
- 作业 ID
- 失败时间和时区
- Source、Transform 和 Sink 连接器名称
- 失败前最近的配置、部署或依赖变更
- 故障是否可以重复，或者是否发生在恢复期间

不要包含密码、令牌或未脱敏的连接信息。

### 收集相关日志

SeaTunnel 默认将进程日志写入 `$SEATUNNEL_HOME/logs`。集群启动脚本会为 Master、Worker 和混合 Server 进程使用不同的日志文件名。有关 Log4j2 配置和按作业路由日志的说明，请参阅[日志](logging.md)。

使用混合日志时，可以通过 `ST-JID` 筛选一个作业。例如：

```shell
JOB_ID=<job-id>
grep -F "[${JOB_ID}]" "$SEATUNNEL_HOME/logs/seatunnel-engine-server.log" > job.log
```

如果启用了按作业路由，请检查 `job-<job-id>.log`。在多节点 Zeta 集群中，需要同时收集 Master 和执行该作业的 Worker 日志。当前 Master 还提供以下 REST 接口：

```text
GET http://<master-host>:8080/logs/<job-id>
GET http://<master-host>:8080/logs?format=json
GET http://<node-host>:5801/log
```

第一个接口从所有 Zeta 节点获取匹配的日志，最后一个接口读取单个节点的日志。如果配置了 context path 或动态 HTTP 端口，接口地址也会变化。完整行为请参阅 [RESTful API V2](rest-api-v2.md#get-logs-from-all-nodes)。

在 Kubernetes 环境中，需要保留所有相关 Master 和 Worker Pod 的日志。先收集故障时间窗口；如果 Pod 发生过重启，还需要收集上一个容器的日志：

```shell
kubectl logs <pod-name> --since=30m
kubectl logs <pod-name> --previous
kubectl describe pod <pod-name>
```

当进程是被 Kubernetes 终止而不是由 Java 异常退出时，`kubectl describe pod` 尤其重要。

### 保留因果上下文

不要只保留 `ERROR` 行，还需要保留：

- 第一次失败之前的警告
- 第一次异常，而不是只有后续重试
- 所有嵌套的 `Caused by` 内容
- 时间戳、Logger 名称、线程名称和 `ST-JID`
- 同一时间窗口内 Worker 或连接器的消息

可以使用下面的命令生成初始片段。如果根因出现在所选范围以外，需要继续扩大上下文。

```shell
grep -n -B 30 -A 80 -E \
  'ERROR|WARN|Caused by|Exception|OutOfMemoryError|timeout checkpoint' job.log \
  > diagnostic-excerpt.log
```

重复的重试消息通常是结果而不是最初原因。应从第一次重试向前查找原始异常。

## 分享前进行脱敏

使用稳定的占位符替换敏感值，这样仍然可以观察它们之间的关系。

| 敏感值 | 替换示例 |
|--------|----------|
| 密码、令牌、密钥、私钥 | `<redacted-secret>` |
| 数据库或消息系统主机名 | `<source-host>` |
| 用户名或账户 ID | `<service-user>` |
| 内部路径或 Bucket 名称 | `<data-path>` |
| SQL 字面量或数据记录 | `<record-value>` |

如果与问题有关，应保留配置项名称、异常类、时间戳、端口和连接 URL 的结构。例如，将 `jdbc:mysql://orders.internal:3306/sales?useSSL=true` 替换为 `jdbc:mysql://<source-host>:3306/<database>?useSSL=true`。

自动替换完成后仍需人工检查。简单的正则表达式无法识别所有凭据和业务数据。

## 提示词模板

以下模板可以用于通用 AI 工具、[SeaTunnel Skill](../../tools/seatunnel-skill.md) 或其他经过批准的辅助工具。

```text
我正在诊断一个 Apache SeaTunnel 作业故障。

运行环境：
- SeaTunnel 版本：<version>
- 执行引擎和部署模式：<engine-and-mode>
- 作业模式：<batch-or-streaming>
- 连接器：<source-transform-sink>
- 失败时间和时区：<timestamp>
- 最近变更：<changes-or-none>

只根据下面提供的证据进行分析。

1. 说明观察到的故障，并找出最早可以采取行动的异常。
2. 引用支持每个结论的准确日志行。
3. 分开列出已确认事实和假设。
4. 对假设排序，并说明缺少哪些证据。
5. 在提出修复方法之前先给出验证步骤。
6. 不要编造 SeaTunnel 配置项。对于需要查阅文档确认的配置项，请明确标记。

已脱敏的日志片段：
<paste-excerpt-here>
```

后续提问时，应补充验证步骤的结果，不要反复发送完整日志。

## 常见故障模式

以下模式只能作为排查起点。相似的消息可能有不同的原因。

### 连接器或 Factory 发现失败

常见证据包括 `FactoryException`、`Unable to create a source` 或 `Could not find any factory for identifier`。

需要验证：

- 作业配置中的连接器标识符
- 所有相关节点是否都安装了连接器插件
- 所有节点是否使用相同的 SeaTunnel 和连接器版本
- 嵌套异常中输出的可用 Factory 标识符列表

### 连接、认证或 TLS 失败

SeaTunnel 外层异常可能包装数据库、消息系统、HTTP 或云 SDK 异常。需要保留完整的 `Caused by` 链，并从实际运行任务的节点验证连接。DNS、端口、TLS 信任、权限和限流都需要独立于 AI 结果进行检查。

### Checkpoint 超时

`CHECKPOINT_EXPIRED` 表示在配置的 Checkpoint 超时时间内没有收到所有必需的确认。直接增加超时时间可能只会隐藏现象，而不会解决根因。

需要检查：

- [繁忙度和反压](busyness-and-backpressure.md)
- Sink 延迟和外部系统状态
- Worker 丢失或长时间垃圾回收暂停
- Checkpoint 历史和没有确认的任务
- 完成上述检查之后再确认 Checkpoint 超时配置

### 内存不足

修改内存配置之前，需要区分以下情况：

- `java.lang.OutOfMemoryError: Java heap space`
- Direct 或 Native Memory 不足
- Kubernetes 容器被标记为 `OOMKilled`
- 主机级内存压力

需要收集 JVM 消息、Pod 终止原因、内存限制、最近的垃圾回收证据和工作负载。不要将堆转储上传到外部 AI 服务。

### 任务重试或 Worker 故障

重复的任务部署、通知或恢复消息描述的是重试流程。需要找到重试之前的第一次异常，并关联同一时间窗口内的 Master 和 Worker 日志。修改重试配置之前，应先确认 Worker 状态和集群成员关系。

## 可重现的演示

下面的示例使用 SeaTunnel 当前 Factory 发现流程中的异常消息。先准备一个可以正常运行的本地作业，然后临时将 Source 连接器标识符替换为 `JdbcTypo`。作业会在创建连接器之前失败。

缩短并脱敏后的异常链如下：

```text
org.apache.seatunnel.api.table.factory.FactoryException:
Unable to create a source for identifier 'JdbcTypo'.
Caused by: org.apache.seatunnel.api.table.factory.FactoryException:
Could not find any factory for identifier 'JdbcTypo' that implements
'org.apache.seatunnel.api.table.factory.TableSourceFactory' in the classpath.

Available factory identifiers are:

...
Jdbc
...
```

外层异常说明失败阶段，嵌套异常提供了可以采取行动的证据：`JdbcTypo` 不可用，而 `Jdbc` 可用。这支持连接器标识符不匹配的判断，但不能证明修正拼写后整个 JDBC 配置一定可以正常工作。

修改作业之前需要验证：

1. 检查提交配置中的 Source Block 标识符。
2. 确认每个节点都安装了预期的连接器。
3. 将标识符与连接器文档和可用标识符列表进行比较。
4. 修正标识符后重新运行作业。
5. 如果出现新的异常，将其作为一个独立故障重新收集证据。

这样可以避免把合理的第一次诊断错误地表述为整个作业配置已经得到验证。

## 何时向社区求助

如果现有证据仍然无法确认原因，请搜索已有 [GitHub Issue](https://github.com/apache/seatunnel/issues) 和[开发者邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)。创建 Issue 时，应提供已脱敏的运行环境、最早异常、相关上下文和已经完成的验证步骤。不要发布原始的未脱敏日志。
