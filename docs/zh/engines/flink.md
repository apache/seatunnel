# SeaTunnel 运行在 Flink 上

当你的团队已经在生产中运行 Flink 集群，并希望 SeaTunnel 作业复用这套运行时平台时，Flink 会是一个很合适的选择。如果你只是第一次评估 SeaTunnel，且没有现成的 Flink 运维体系，建议先从 [SeaTunnel Engine](./zeta/about.md) 开始，再根据实际需要回到这一页。

## 从这里开始

如果你的目标是让 SeaTunnel 跑在 Flink 上，建议按下面顺序阅读：

- [引擎概览](./overview.md)
- [Flink 引擎快速开始](../getting-started/locally/quick-start-flink.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

## 什么时候选择 Flink

以下场景通常更适合使用 Flink：

- 团队已经长期运行 Flink 集群
- 希望复用已有的 Flink 部署、监控与运维体系
- 作业需要融入更大的 Flink 流处理环境

## Flink 专属配置如何写

SeaTunnel 作业里的 Flink 专属配置，需要写在 `env` 块中，并使用 `flink.` 前缀。

示例：

```hocon
env {
  parallelism = 1
  flink.execution.checkpointing.unaligned.enabled = true
}
```

某些枚举类配置项暂时不适合直接内联在 SeaTunnel 作业配置里，遇到这类参数时，建议放到 Flink 自身配置中。当前常见的内联支持类型主要包括：

- `Integer`
- `Boolean`
- `String`
- `Duration`

## Flink 1.20.4 / Java 17 运行前检查

以下条件已在 Flink 1.20.4 / Java 17 下验证。请检查本作业实际使用的 Flink 安装，不能用其他版本或其他集群的配置替代。

1. 记录提交客户端的 `JAVA_HOME`、`PATH` 和 JVM 参数，以及 JobManager、TaskManager 的启动配置。同时检查正在运行的集群进程与配置文件：修改文件不会改变已启动 JVM 的参数。SeaTunnel 启动脚本会加载 `$SEATUNNEL_HOME/config/seatunnel-env.sh`，因此需要核实加载后的有效 `FLINK_HOME` 和 Java 设置。
2. 在 Java 17 提交客户端和 TaskManager JVM 参数中追加 `--add-opens=java.base/java.util=ALL-UNNAMED`，保留已有参数。缺少该参数时，客户端可能在注册 process-function translator 时失败，TaskManager 也可能在 Kryo/Chill 序列化时抛出 `InaccessibleObjectException`。通过实际管理该安装的进程管理器或启动环境配置 TaskManager 参数，然后重启受影响的进程。隔离环境手动启动时，可在客户端和集群的启动环境中分别追加 `JAVA_TOOL_OPTIONS`：

   ```bash
   export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }--add-opens=java.base/java.util=ALL-UNNAMED"
   ```

   只配置客户端不会更新已运行的 TaskManager。已验证的 standalone 环境在 JobManager 和 TaskManager 的启动环境中均设置了该参数；实际反射访问失败要求客户端和 TaskManager 配置该参数。
3. 启用 lineage 时，在 JobManager 的 `$FLINK_HOME/lib` 中安装唯一且版本匹配的 `seatunnel-lineage-flink-<version>-shaded.jar`，然后重启 JobManager。检查 jar 内包含 `org/apache/seatunnel/lineage/flink/LineageJobStatusHook.class`。仅随提交的作业 jar 发送 hook 不足以使其加载。配置和 token 处理参见 [lineage 制品安装说明](../introduction/concepts/lineage.md)。
4. 提交前查询目标集群的 REST API：

   ```bash
   curl --fail --silent --show-error "$FLINK_REST_URL/overview"
   curl --fail --silent --show-error "$FLINK_REST_URL/taskmanagers"
   ```

   确认 `flink-version` 为预期版本，至少有一个已注册的 TaskManager，且 `slots-available` 足以满足作业 parallelism。JobManager 可访问但 TaskManager 或 slot 为零时，还不能执行作业。

### 复现与恢复

使用已知源数据集的有界作业，目标端需支持完整读回。先安装所需连接器，再通过 Flink 1.20 启动脚本提交：

```bash
# Attached
bash "$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh" \
  --config "$JOB_CONFIG" --target remote -n "$JOB_NAME"
# Detached：本次运行使用独立的目标和作业名
bash "$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh" \
  --config "$DETACHED_JOB_CONFIG" --target remote -n "$DETACHED_JOB_NAME" -d
```

每次提交记录 JobID，等待 `FINISHED`，比较目标端与源端全部排序行，并检查每个预期 lineage run 恰好一个 `START` 和最终 `COMPLETE`。Detached 提交成功只代表已提交，还需检查 JobManager 的结果和 receiver 事件。完成后再次读取事件，确认没有重复终态或晚到 heartbeat。

在隔离测试安装中，还应验证移除 shaded hook jar 并重启 JobManager 后，enabled 作业在提交时明确失败；`openlineage_enabled=false` 时数据作业仍能完成，且不产生 lineage 事件。

修改安装前，保存配置、lib jar 列表、启动环境及运行角色/slot 状态。记录精确改动和重启命令。测试结束后恢复配置、jar 和启动设置，只启动原来运行的角色，再与保存的基线比较。若原基线为 TaskManager=0、slots=0，恢复到该状态是正确的，虽然它不满足测试提交条件。

## 最小示例作业

下面这个例子会在 Flink 上运行，并把生成的数据打印到控制台。

```hocon
env {
  parallelism = 1
  checkpoint.interval = 5000

  flink.execution.checkpointing.mode = "EXACTLY_ONCE"
  flink.execution.checkpointing.timeout = 600000
}

source {
  FakeSource {
    row.num = 16
    plugin_output = "fake_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(33, 18)"
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake_table"
    plugin_output = "fake_output"
    field_mapper = {
      c_string = c_string
      c_int = c_int
    }
  }
}

sink {
  Console {
    plugin_input = "fake_output"
  }
}
```

如果你需要更多 transform 能力，继续查看 [Transforms 目录](../transforms) 和 [Transform 通用参数](../transforms/common-options/common-options.md)。

## 从源码仓库运行示例

如果你是在源码仓库里运行示例，对应模块是：

- `seatunnel-examples/seatunnel-flink-connector-v2-example`

示例入口类是：

- `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample`

## 下一步

- [Flink 引擎快速开始](../getting-started/locally/quick-start-flink.md)
- [Flink 转换层](../architecture/api-design/flink-translation-layer.md)
- [Transforms 目录](../transforms)
- 如果你还想和默认引擎对比，可回看 [SeaTunnel Engine](./zeta/about.md)
