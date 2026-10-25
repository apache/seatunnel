---
slug: /connectors/formats
sidebar_position: 1
---

# 数据格式

格式文档关注的是 SeaTunnel 内部数据模型与外部编码之间如何对应，例如 Avro、Debezium JSON、Protobuf 等。当连接器本身还不够，任务还需要你明确控制消息体、Schema 或 CDC 包装格式时，就进入这一节。

## 什么时候需要看格式文档

- source 或 sink 读写的是带 Schema 的外部载荷
- CDC 链路依赖特定的 envelope 格式
- 需要让外部序列化格式与下游消费方保持一致

## 常用下一步

- [数据格式处理](../../architecture/data-format-handling.md)
- [连接器常见问题](../connector-faq.md)
