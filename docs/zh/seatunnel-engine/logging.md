---
sidebar_position: 14
---

# 日志

每个 SeaTunnel Engine 进程都会创建一个日志文件，其中包含该进程中发生的各种事件的消息。这些日志提供了对 SeaTunnel Engine 内部工作原理的深入了解，可用于检测问题（以 WARN/ERROR 消息的形式）并有助于调试问题。

SeaTunnel Engine 中的日志记录使用 SLF4J 日志记录接口。这允许您使用任何支持 SLF4J 的日志记录框架，而无需修改 SeaTunnel Engine 源代码。

默认情况下，Log4j2 用作底层日志记录框架。

## 结构化信息

SeaTunnel Engine 向大多数相关日志消息的 MDC 添加了以下字段（实验性功能）：

- Job ID
  - key: ST-JID
  - format: string

这在具有结构化日志记录的环境中最为有用，允许您快速过滤相关日志。

MDC 由 slf4j 传播到日志后端，后者通常会自动将其添加到日志记录中（例如，在 log4j json 布局中）。或者，也可以明确配置 - log4j 模式布局可能如下所示：

```properties
[%X{ST-JID}] %c{0} %m%n.
```

## 配置 Log4j2

Log4j2 使用属性文件进行控制。

SeaTunnel Engine 发行版在 `confing` 目录中附带以下 log4j 属性文件，如果启用了 Log4j2，则会自动使用这些文件：

- `log4j2_client.properties`: 由命令行客户端使用 (e.g., `seatunnel.sh`)
- `log4j2.properties`: 由 SeaTunnel 引擎服务使用 (e.g., `seatunnel-cluster.sh`)

默认情况下，日志文件输出到 `logs` 目录。

Log4j 会定期扫描上述文件以查找更改，并根据需要调整日志记录行为。默认情况下，此检查每 60 秒进行一次，由 Log4j 属性文件中的 monitorInterval 设置控制。

### 配置作业生成单独的日志文件

要为每个作业输出单独的日志文件，您可以更新 `log4j2.properties` 文件中的以下配置：

```properties
...
rootLogger.appenderRef.file.ref = routingAppender
...

appender.file.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%-30.30c{1.}] [%t] - %m%n
...
```

此配置为每个作业生成单独的日志文件，例如：

```
job-xxx1.log
job-xxx2.log
job-xxx3.log
...
```

### 配置混合日志文件

*默认已采用此配置模式。*

要将所有作业日志输出到 SeaTunnel Engine 系统日志文件中，您可以在 `log4j2.properties` 文件中更新以下配置：

```properties
...
rootLogger.appenderRef.file.ref = fileAppender
...

appender.file.layout.pattern = [%X{ST-JID}] %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%-30.30c{1.}] [%t] - %m%n
...
```

### 兼容 Log4j1/Logback

SeaTunnel Engine 自动集成了大多数 Log 桥接器，允许针对 Log4j1/Logback 类工作的现有应用程序继续工作。

### REST-API方式查询日志

SeaTunnel 提供了一个 API，用于查询日志。

**使用样例：**
- 获取所有节点jobId为`733584788375666689`的日志信息：`http://localhost:5801/hazelcast/rest/maps/logs/733584788375666689`
- 获取所有节点日志列表：`http://localhost:5801/hazelcast/rest/maps/logs`
- 获取所有节点日志列表以JSON格式返回：`http://localhost:5801/hazelcast/rest/maps/logs?format=json`
- 获取日志文件内容：`http://localhost:5801/hazelcast/rest/maps/logs/job-898380162133917698.log`

有关详细信息，请参阅 [REST-API](rest-api-v2.md)。

## 开发人员最佳实践

您可以通过调用 `org.slf4j.LoggerFactory#LoggerFactory.getLogger` 并以您的类的类作为参数来创建 SLF4J 记录器。

当然您也可以使用 lombok 注解 `@Slf4j` 来实现同样的效果

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConnector {
	private static final Logger LOG = LoggerFactory.getLogger(TestConnector.class);

	public static void main(String[] args) {
		LOG.info("Hello world!");
	}
}
```

为了最大限度地利用 SLF4J，建议使用其占位符机制。使用占位符可以避免不必要的字符串构造，以防日志级别设置得太高而导致消息无法记录。

占位符的语法如下：

```java
LOG.info("This message contains {} placeholders. {}", 1, "key1");
```

占位符还可以与需要记录的异常结合使用

```java
try {
    // some code
} catch (Exception e) {
    LOG.error("An {} occurred", "error", e);
}
```