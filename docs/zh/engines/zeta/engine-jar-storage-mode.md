---
sidebar_position: 9
---

# 配置引擎 Jar 存储模式

:::caution 警告

请注意，此功能目前处于实验阶段，还有许多方面需要改进。因此，我们建议在使用此功能时谨慎行事，以避免潜在的问题和不必要的风险。
我们致力于持续努力增强和稳定此功能，确保为您提供更好的体验。

:::

我们可以启用优化的作业提交过程，这在 `seatunnel.yaml` 中进行配置。启用了 Seatunnel 作业提交过程配置项的优化后，
用户可以使用 Seatunnel Zeta 引擎作为执行引擎，而无需在每个引擎 `connector` 目录中放置任务执行所需的连接器 Jar 包或连接器所依赖的第三方 Jar 包。
用户只需在提交作业的客户端上放置所有任务执行所需的 Jar 包，客户端将自动上传任务执行所需的 Jars 到 Zeta 引擎。在 Docker 或 k8s 模式下提交作业时，启用此配置项是必要的，
这可以从根本上解决由 Seatunnel Zeta 引擎的重量造成的大型容器镜像问题。在镜像中，只需要提供 Zeta 引擎的核心框架包，
然后可以将连接器的 jar 包和连接器所依赖的第三方 jar 包分别上传到 pod 进行分发。

启用了优化作业提交过程配置项后，您不需要在 Zeta 引擎中放置以下两种类型的 Jar 包：
- COMMON_PLUGIN_JARS
- CONNECTOR_PLUGIN_JARS

COMMON_ PLUGIN_ JARS 指的是连接器所依赖的第三方 Jar 包， CONNECTOR_ PLUGIN_ JARS 指的是连接器 Jar 包。
当 Zeta 的 `lib` 中不存在公共 jars 时，它可以将客户端的本地公共 jars 上传到所有引擎节点的 `lib` 目录。
这样，即使用户没有在 Zeta 的 `lib` 中放置 jar，任务仍然可以正常执行。
然而，我们不推荐依赖打开优化作业提交过程的配置项来上传连接器所依赖的第三方 Jar 包。
如果您使用 Zeta 引擎，请将连接器所依赖的第三方 jar 包文件添加到每个节点的 `$SEATUNNEL_HOME/lib/` 目录中，例如 jdbc 驱动程序。

# 连接器 Jar 存储策略

您可以通过配置文件配置当前连接器 Jar 包和连接器所依赖的第三方 Jar 包的存储策略。
可以配置两种存储策略，即共享 Jar 包存储策略和隔离 Jar 包存储策略。
两种不同的存储策略为 Jar 文件提供了更灵活的存储模式。
您可以配置存储策略，使引擎中的多个执行作业共享相同的 Jar 包文件。

## 相关配置

|                 参数                  |  默认值   |                                   描述                                    |
|-------------------------------------|--------|-------------------------------------------------------------------------|
| connector-jar-storage-enable        | false  | 是否启用上传连接器 Jar 包到引擎。默认启用状态为 false。                                       |
| connector-jar-storage-mode          | SHARED | 引擎端 Jar 包存储模式选择。有两个可选模式，SHARED（共享）和 ISOLATED（隔离）。默认的 Jar 包存储模式是 SHARED。 |
| connector-jar-storage-path          | " "    | 用户自定义的 Jar 包存储路径。                                                       |
| connector-jar-cleanup-task-interval | 3600s  | 引擎端 Jar 包清理定时任务执行间隔。                                                    |
| connector-jar-expiry-time           | 600s   | 引擎端 Jar 包存储过期时间。                                                        |

## 隔离连接器Jar存储策略

在作业提交之前，连接器 Jar 包将被上传到 Master 节点上的一个独立文件存储路径中。
不同作业的连接器 Jar 包位于不同的存储路径中，因此不同作业的连接器 Jar 包彼此隔离。
作业执行所需的 Jar 包文件不会影响其他作业。当当前作业执行结束时，基于 `JobId` 生成的存储路径中的 Jar 包文件将被删除。

示例：

```yaml
jar-storage:
   connector-jar-storage-enable: true
   connector-jar-storage-mode: ISOLATED
   connector-jar-storage-path: ""
   connector-jar-cleanup-task-interval: 3600
   connector-jar-expiry-time: 600
```

配置参数的详细解释：
- connector-jar-storage-enable: 在执行作业前启用上传连接器 Jar 包的功能。
- connector-jar-storage-mode: 连接器 Jar 包的存储模式，有两种存储模式可供选择：共享模式（SHARED）和隔离模式（ISOLATED）。
- connector-jar-storage-path: 在 Zeta 引擎上用户自定义连接器 Jar 包的本地存储路径。
- connector-jar-cleanup-task-interval: Zeta 引擎连接器 Jar 包定时清理任务的间隔时间，默认为 3600 秒。
- connector-jar-expiry-time: 连接器 Jar 包的过期时间，默认为 600 秒。

## 共享连接器Jar存储策略

在作业提交之前，连接器 Jar 包将被上传到 Master 节点。如果不同的作业使用相同的 Jar 包文件，它们可以在 Master 节点上共享连接器 Jars。
所有 Jar 包文件都被持久化到一个共享的文件存储路径中，引用 Master 节点的 Jar 包可以在不同作业之间共享。任务执行完成后，
共享连接器Jar存储策略 不会立即删除与当前任务执行相关的所有 Jar 包，而是有一个独立的线程负责清理工作。
以下配置文件中的配置设置了清理工作的运行时间和 Jar 包的存活时间。

示例:

```yaml
jar-storage:
   connector-jar-storage-enable: true
   connector-jar-storage-mode: SHARED
   connector-jar-storage-path: ""
   connector-jar-cleanup-task-interval: 3600
   connector-jar-expiry-time: 600
```

配置参数的详细解释：
- connector-jar-storage-enable: 在执行作业前启用上传连接器 Jar 包的功能。
- connector-jar-storage-mode: 连接器 Jar 包的存储模式，有两种存储模式可供选择：共享模式（SHARED）和隔离模式（ISOLATED）。
- connector-jar-storage-path: 在 Zeta 引擎上用户自定义连接器 Jar 包的本地存储路径。
- connector-jar-cleanup-task-interval: Zeta 引擎连接器 Jar 包定时清理任务的间隔时间，默认为 3600 秒。
- connector-jar-expiry-time: 连接器 Jar 包的过期时间，默认为 600 秒。
