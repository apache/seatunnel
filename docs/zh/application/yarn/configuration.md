---
sidebar_position: 4
title: 配置参考
---

# YARN 配置参考

部署配置使用 HOCON。所有数值资源和超时必须为正数，Master 端口必须在 `1-65535` 范围内。

## YARN 选项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `yarn.deployment-target` | Enum | `APPLICATION` | 部署拓扑；一期仅支持每个作业对应一个独立 application。 |
| `yarn.distribution` | String | 必填 | 提交机可读的 `.tar.gz`、`.tgz` 或 `.zip` 发行包。 |
| `yarn.config-dir` | String | 空 | Hadoop 配置目录；为空时使用 `HADOOP_CONF_DIR`，再回退到 Hadoop classpath。 |
| `yarn.staging-dir` | String | `.seatunnel/applications` | 共享文件系统 staging 根目录；相对路径通常位于提交用户的 HDFS home。 |
| `yarn.queue` | String | `default` | 提交 application 的 YARN 队列。 |
| `yarn.priority` | Integer | `-1` | Application 调度优先级；负数表示沿用集群默认值。 |
| `yarn.tags` | String | 空 | 以逗号分隔的 YARN application tag。 |
| `yarn.master.node-label` | String | 空 | ApplicationMaster 使用的 node-label expression。 |
| `yarn.worker.node-label` | String | 空 | Worker 使用的 node-label expression；为空时继承 `yarn.master.node-label`。 |

`yarn.config-dir` 只用于配置 Hadoop 客户端。SeaTunnel 运行时与日志配置来自 `yarn.distribution` 内的 `config/seatunnel.yaml` 和 `config/log4j2_client.properties`。YARN 会为 ApplicationMaster 和所有 Worker 本地化同一份只读发行包；需要自定义 JVM、引擎、checkpoint 或日志配置时，应在创建归档前修改这些文件。

## Application 公共选项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `application.name` | String | `seatunnel` | YARN application 显示名称。 |
| `application.job-id` | Long | 自动生成 | 正数原生 Zeta job ID。 |
| `application.restore-job-id` | Long | 未设置 | 从此历史 job ID 的最新有效 checkpoint 恢复。 |
| `application.worker-count` | Integer | `1` | 固定 Worker Container 数量。 |
| `application.worker.memory-mb` | Integer | `1024` | 每个 Worker 的 Container 内存，单位 MiB。 |
| `application.worker.cpu-cores` | Integer | `1` | 每个 Worker 的虚拟 CPU 核数。 |
| `application.worker.slots` | Integer | `2` | 每个 Worker 的固定执行 slot 数。 |
| `application.master.memory-mb` | Integer | `1024` | ApplicationMaster Container 内存，单位 MiB。 |
| `application.master.cpu-cores` | Integer | `1` | ApplicationMaster 虚拟 CPU 核数。 |
| `application.master.port` | Integer | `5801` | Hazelcast 起始端口；冲突时选择后续可用端口。 |
| `application.startup-timeout-millis` | Long | `120000` | 分别用于等待 AM 启动，以及 Worker 申请和注册。 |

Container JVM 堆使用申请内存的 75%，剩余空间用于堆外内存和 JVM 开销。最小申请内存为 64 MiB。

`application.startup-timeout-millis` 不是作业执行超时。流作业可以持续运行；该值只限制客户端等待 AM，以及 Master 等待 Worker 分配和注册的时间。

## 容量规划

同时规划 Worker 数、每个 Worker 的 slot 和作业并行度。Source、Transform 和 Sink 都可能产生 task group，因此仅按 `env.parallelism` 估算 slot 可能不足。资源请求还不能超过 YARN 最大 Container 能力和队列配额。

## 配置覆盖

命令行 `-Dkey=value` 的优先级高于部署配置：

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config job.conf --deployment-config yarn-deployment.conf \
  -Dapplication.worker-count=3 \
  -Dapplication.worker.slots=4
```
