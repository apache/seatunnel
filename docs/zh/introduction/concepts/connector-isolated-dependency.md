# Connector 依赖隔离加载机制

SeaTunnel 提供了针对每个 connector 的依赖隔离加载机制，方便用户管理不同连接器单独的依赖，同时避免依赖冲突并提升系统的可扩展性。
当加载 connector 时，SeaTunnel 会从 `${SEATUNNEL_HOME}` 下的 `plugins/connector-xxx` 目录中，查找并加载该 connector 独立的依赖 jar。这种方式确保了不同 connector 所需的依赖不会相互影响，便于在复杂环境下管理大量 connector。

## 实现原理

每个 connector 需要将自己的依赖 jar 放置在 `${SEATUNNEL_HOME}/plugins/connector-xxx` 目录下的独立子目录中（需要手动创建）。
子目录名称由 `plugin-mapping` 文件中的 value 值指定。SeaTunnel 启动并加载 connector 时，只会加载对应目录下的 jar，从而实现依赖的隔离。

目前，Zeta 引擎会保证同一个任务不同connector的jar分开加载。其他两个引擎仍然会将所有 connector 的依赖 jar 一起加载，同一个任务放置了不同版本的jar在Spark/Flink环境可能导致依赖冲突。

## 目录结构示例

- 通过`${SEATUNNEL_HOME}/connectors/plugin-mapping.properties` 获取每个connector对应的文件夹目录命名。

以AmazonDynamodb为例，假设在 `plugin-mapping` 文件中有以下配置：
```
seatunnel.source.AmazonDynamodb = connector-amazondynamodb
```

则对应的connector依赖目录就是value值 `connector-amazondynamodb`。

最终的目录结构如下所示：

```
SEATUNNEL_HOME/
  plugins/
    connector-amazondynamodb/
      dependency1.jar
      dependency2.jar
    connector-xxx/
      dependencyA.jar
      dependencyB.jar
```

## 限制说明

- 在Zeta引擎中，请确保所有节点的 `${SEATUNNEL_HOME}/plugins/` 目录结构一致。都需要包含相同的子目录和依赖 jar。
- 任何没有以`connector-`开头的目录或者jar都将被当作通用依赖目录处理，所有引擎和connector都会加载此类jar。
- 在Zeta引擎中，可以通过将通用的jar放到 `${SEATUNNEL_HOME}/lib/` 目录下来实现所有 connector 的共享依赖。

## 验证

- 通过追踪任务日志，确认每个 connector 只加载了其独立的依赖 jar。

    ```log
    2025-08-13T17:55:48.7732601Z [] 2025-08-13 17:55:47,270 INFO  org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery - find connector jar and dependency for PluginIdentifier{engineType='seatunnel', pluginType='source', pluginName='Jdbc'}: [file:/tmp/seatunnel/plugins/Jdbc/lib/vertica-jdbc-12.0.3-0.jar, file:/tmp/seatunnel/connectors/connector-jdbc-2.3.13-SNAPSHOT-2.12.15.jar]
    ```

