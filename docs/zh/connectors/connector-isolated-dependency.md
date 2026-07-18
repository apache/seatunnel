# 连接器依赖隔离加载机制

SeaTunnel 提供了针对每个连接器的依赖隔离加载机制，方便用户管理不同连接器各自的依赖，同时避免依赖冲突并提升系统可扩展性。
当加载连接器时，SeaTunnel 会从 `${SEATUNNEL_HOME}` 下的 `plugins/connector-xxx` 目录中，查找并加载该连接器独立的依赖 jar。这种方式确保不同连接器所需的依赖不会相互影响，便于在复杂环境下管理大量连接器。

## 实现原理

每个连接器都需要将自己的依赖 jar 放置在 `${SEATUNNEL_HOME}/plugins/connector-xxx` 目录下的独立子目录中（需要手动创建）。
子目录名称由 `plugin-mapping` 文件中的 value 值指定。SeaTunnel 启动并加载连接器时，只会加载对应目录下的 jar，从而实现依赖隔离。

目前，Zeta 引擎会保证同一个任务中的不同连接器 jar 分开加载。其他两个引擎仍然会将所有连接器依赖 jar 一起加载，同一个任务如果放置了不同版本的 jar，在 Spark/Flink 环境中可能导致依赖冲突。

## 目录结构示例

- 通过 `${SEATUNNEL_HOME}/connectors/plugin-mapping.properties` 获取每个连接器对应的目录名称。

以AmazonDynamodb为例，假设在 `plugin-mapping` 文件中有以下配置：
```
seatunnel.source.AmazonDynamodb = connector-amazondynamodb
```

则对应的连接器依赖目录就是 value 值 `connector-amazondynamodb`。

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

- 在 Zeta 引擎中，请确保所有节点的 `${SEATUNNEL_HOME}/plugins/` 目录结构一致，都包含相同的子目录和依赖 jar。
- 任何没有以 `connector-` 开头的目录或 jar 都会被当作通用依赖目录处理，所有引擎和连接器都会加载此类 jar。
- 在 Zeta 引擎中，可以通过将通用 jar 放到 `${SEATUNNEL_HOME}/lib/` 目录下，实现所有连接器共享依赖。

## 验证

- 通过追踪任务日志，确认每个连接器只加载了自己独立的依赖 jar。

    ```log
    2025-08-13T17:55:48.7732601Z [] 2025-08-13 17:55:47,270 INFO  org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery - find connector jar and dependency for PluginIdentifier{engineType='seatunnel', pluginType='source', pluginName='Jdbc'}: [file:/tmp/seatunnel/plugins/Jdbc/lib/vertica-jdbc-12.0.3-0.jar, file:/tmp/seatunnel/connectors/connector-jdbc-3.0.0-SNAPSHOT-2.12.15.jar]
    ```
