# Connector V2 功能简介

## Connector V2 和 V1 之间的不同

从 https://github.com/apache/seatunnel/issues/1608 我们添加了 Connector V2 特性。
Connector V2 是基于SeaTunnel Connector API接口定义的连接器。不像Connector V1， V2 支持如下特性：

* **多引擎支持** SeaTunnel Connector API 是引擎独立的API。基于这个API开发的连接器可以在多个引擎上运行。目前支持Flink和Spark引擎，后续我们会支持其它的引擎。
* **多引擎版本支持** 通过翻译层将连接器与引擎解耦，解决了大多数连接器需要修改代码才能支持新版本底层引擎的问题。
* **流批一体** Connector V2 可以支持批处理和流处理。我们不需要为批和流分别开发连接器。
* **多路复用JDBC/Log连接。** Connector V2支持JDBC资源复用和共享数据库日志解析。

## Source Connector 特性

Source connector有一些公共的核心特性，每个source connector在不同程度上支持它们。

### 精确一次（exactly-once）

如果数据源中的每条数据仅由源向下游发送一次，我们认为该source connector支持精确一次（exactly-once）。

在SeaTunnel中, 我们可以保存读取的 **Split** 和它的 **offset**(当时读取的数据被分割时的位置，例如行号, 字节大小, 偏移量等) 作为检查点时的 **StateSnapshot** 。 如果任务重新启动, 我们会得到最后的 **StateSnapshot**
然后定位到上次读取的 **Split** 和 **offset**，继续向下游发送数据。

例如 `File`, `Kafka`。

### 列投影（column projection）

如果连接器支持仅从数据源读取指定列（请注意，如果先读取所有列，然后通过元数据（schema）过滤不需要的列，则此方法不是真正的列投影）

例如 `JDBCSource` 可以使用sql定义读取列。

`KafkaSource` 从主题中读取所有内容然后使用`schema`过滤不必要的列, 这不是真正的`列投影`。

### 批（batch）

批处理作业模式，读取的数据是有界的，当所有数据读取完成后作业将停止。

### 流（stream）

流式作业模式，数据读取无界，作业永不停止。

### 并行性（parallelism）

并行执行的Source Connector支持配置 `parallelism`，每个并发会创建一个任务来读取数据。
在**Parallelism Source Connector**中，source会被分割成多个split，然后枚举器会将 split 分配给 SourceReader 进行处理。

### 支持用户自定义split

用户可以配置分割规则。

### 支持多表读取

支持在一个 SeaTunnel 作业中读取多个表。

## Sink Connector 的特性

Sink connector有一些公共的核心特性，每个sink connector在不同程度上支持它们。

### 精确一次（exactly-once）

当任意一条数据流入分布式系统时，如果系统在整个处理过程中仅准确处理任意一条数据一次，且处理结果正确，则认为系统满足精确一次一致性。

对于sink connector，如果任何数据只写入目标一次，则sink connector支持精确一次。 通常有两种方法可以实现这一目标：

* 目标数据库支持key去重。例如 `MySQL`, `Kudu`。
* 目标支持 **XA 事务**(事务可以跨会话使用，即使创建事务的程序已经结束，新启动的程序也只需要知道最后一个事务的ID就可以重新提交或回滚事务）。 然后我们可以使用 **两阶段提交** 来确保 * 精确一次**。 例如：`File`, `MySQL`.

### cdc(更改数据捕获，change data capture)

如果sink connector支持基于主键写入行类型（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE），我们认为它支持cdc（更改数据捕获，change data capture）。
