---
slug: /connectors/source
sidebar_position: 1
---

# 数据来源连接器

当你的第一个问题是“SeaTunnel 要从哪里读取数据”时，就先看这一页。更好的顺序是先按外部系统找到合适的 Source，再确认插件安装、驱动依赖，以及当前任务是否需要批处理、流处理或 CDC 语义。

## 选择 Source 前先确认什么

- 你实际要读取的是哪一种外部系统
- 任务需要快照、增量还是 CDC 语义
- 是否依赖额外驱动、SDK 或鉴权配置
- 示例参数是否与你当前使用的 SeaTunnel 版本一致

## 常用下一步

- [Source 常用选项](../common-options/source-common-options.md)
- [CDC 生产实战手册](../cdc-production-cookbook.md)
- [连接器常见问题](../connector-faq.md)
