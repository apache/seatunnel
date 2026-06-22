---
slug: /connectors/sink
sidebar_position: 1
---

# 数据写入连接器

当你的第一个问题是“SeaTunnel 最终要把数据写到哪里”时，就先看这一页。更稳妥的顺序是先匹配目标系统，再确认写入保证、表结构行为、驱动依赖，以及连接器本身的投递约束。

## 选择 Sink 前先确认什么

- 最终目标系统以及表、对象或主题的落点形态
- 任务需要至少一次、精确一次还是幂等写入
- 是否依赖额外驱动、SDK 或云鉴权配置
- 该连接器是否支持你要使用的写入模式

## 常用下一步

- [Sink 常用选项](../common-options/sink-common-options.md)
- [连接器常见问题](../connector-faq.md)
- [连接器依赖隔离加载机制](../connector-isolated-dependency.md)
