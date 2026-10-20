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

## JSON 中的 DECIMAL 精度

当 Schema 中声明了 `DECIMAL` 字段（包括数组中的嵌套字段）时，JSON 读取器会按 `BigDecimal` 解析浮点数，避免丢失数值精度。支持的类型包括 `ARRAY<DECIMAL>`、`ARRAY<ARRAY<DECIMAL>>` 和 `ARRAY<ROW<DECIMAL>>`。该设置作用于整个文档：同一 Schema 中赋给 `STRING` 字段的裸浮点数会根据十进制数值重新生成文本，可能保留更多位数，但不保证原始写法不变，例如 `0.0000001234` 可能变为 `1.234E-7`。作为文本赋给 `STRING` 字段的 JSON 对象或数组同理：其中的浮点数同样按十进制值重新生成文本，因此文本也可能变化；而这些对象或数组内部带引号的数字仍保持原始文本。带引号的字符串以及不含 `DECIMAL` 的 Schema 保持原有行为。

## 常用下一步

- [数据格式处理](../../architecture/data-format-handling.md)
- [连接器常见问题](../connector-faq.md)
