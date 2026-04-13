# 开发自己的Connector

如果你想针对SeaTunnel新的连接器API开发自己的连接器（Connector V2），请查看[这里](https://github.com/apache/seatunnel/blob/dev/seatunnel-connectors-v2/README.zh.md) 。

## 从这里开始

如果你要开发新的 connector，建议把本页当成目录页，再按场景进入对应实现指南：

- [Source Connector 开发指南](./source-connector-development.md)，面向 source 侧设计与实现
- [Sink Connector 开发指南](./sink-connector-development.md)，面向 sink 侧提交语义与恢复设计
- [插件发现与类加载](../architecture/plugin-discovery-and-class-loading.md)，用于理解 SPI、打包和依赖隔离
- [配置与 Option 系统](../architecture/configuration-and-option-system.md)，用于稳定定义用户可见参数
- [CDC Pipeline 架构概览](../architecture/cdc-pipeline-architecture.md)，适用于 CDC 类 connector

## 架构文档参考

如需了解 SeaTunnel 的 API 设计和引擎架构的详细信息，请参阅：

- [架构概览](../architecture/overview.md) - 整体架构和设计原则
- [数据源架构](../architecture/api-design/source-architecture.md) - Source API 设计深入剖析
- [数据汇架构](../architecture/api-design/sink-architecture.md) - Sink API 设计深入剖析
- [转换层](../architecture/api-design/translation-layer.md) - 连接器如何在不同引擎上工作
- [检查点机制](../architecture/fault-tolerance/checkpoint-mechanism.md) - 容错和状态管理

这些文档将帮助你理解 SeaTunnel 连接器中使用的底层架构和设计模式。

