# Connector V2 功能简介

## Connector V2 和 V1 的区别

SeaTunnel 在 [issue#1608](https://github.com/apache/seatunnel/issues/1608) 中引入了 Connector V2 特性。
Connector V2 基于 SeaTunnel Connector API 接口开发，相比 Connector V1，具有以下优势：

* **多引擎支持：** SeaTunnel Connector API 是一套与引擎无关的接口定义。基于该接口开发的连接器可在多个引擎上运行。目前已支持 Flink 和 Spark 引擎，后续将支持更多引擎。
* **多引擎版本支持：** 通过引入翻译层实现连接器与引擎的解耦，解决了连接器需要频繁修改代码以适配新版本引擎的问题。
* **流批一体：** Connector V2 同时支持批处理和流处理模式，无需为不同模式单独开发连接器。
* **资源复用：** Connector V2 支持 JDBC 连接复用以及数据库日志解析的共享机制。
* **多模态数据集成：** Connector V2 支持多种数据类型的集成，包括结构化数据、非结构化文本、视频、图像及二进制文件等。

## Source Connector 特性

Source connector 具有一系列核心特性，各个连接器对这些特性的支持程度不同。

### 精确一次（Exactly-Once）

当 source connector 能够确保每条数据仅被发送一次到下游时，即具备精确一次特性。

SeaTunnel 通过在检查点中保存 **Split** 和其 **offset**（数据读取位置，如行号、字节位置、偏移量等）作为 **StateSnapshot**，实现精确一次语义。当任务重启时，系统会根据最近的 **StateSnapshot** 恢复到上次的读取位置，继续发送数据。

典型支持示例：`File`、`Kafka`。

### 列投影（Column Projection）

连接器支持仅从数据源读取指定列时，称为支持列投影。注意，如果是先读取所有列再通过元数据（schema）过滤不需要的列，则不属于真正的列投影。

例如，`JDBCSource` 可以通过 SQL 定义读取列；而 `KafkaSource` 则是从主题中读取所有内容后再使用 `schema` 过滤不必要的列，后者不是真正的列投影。

### 批（Batch）

批处理作业模式，读取的数据是有界的，当所有数据读取完成后作业将停止。

### 流（Stream）

流式作业模式，数据读取无界，作业永不停止。

### 并行性（Parallelism）

支持配置 `parallelism` 的 Source Connector 可以并行执行，每个并发实例会创建一个任务来读取数据。
在**Parallelism Source Connector**中，source 会被分割成多个 split，然后枚举器会将 split 分配给 SourceReader 进行处理。

### 多模态（Multimodal）

支持多种数据类型的集成，包括结构化数据、非结构化文本、视频、图像及二进制文件等。

### 支持用户自定义分片

允许用户自定义数据分片规则。

### 支持多表读取

支持在单个 SeaTunnel 作业中读取多张数据表。

## Sink Connector 特性

Sink connector 具有一系列核心特性，各个连接器对这些特性的支持程度不同。

### 精确一次（Exactly-Once）

在分布式系统中，当任何一条数据在整个处理流程中只被准确处理一次，且结果正确时，即实现了精确一次语义。

对于 sink connector，精确一次意味着任何数据都只会被写入目标系统一次。通常可通过以下两种方式实现：

* 目标数据库支持键值去重，如 `MySQL`、`Kudu` 等。
* 目标系统支持 **XA 事务**（事务可跨会话使用，即使创建事务的程序已终止，新程序只需知道最后的事务 ID 即可提交或回滚事务）。通过 **两阶段提交** 机制确保精确一次，如 `File`、`MySQL` 等。

### CDC（变更数据捕获）

支持基于主键的行级操作（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE）的 sink connector 具备 CDC 能力。

### 多表写入

支持在单个 SeaTunnel 作业中写入多张表，用户可通过[配置占位符](./sink-options-placeholders.md)动态指定表标识。
