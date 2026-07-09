---
slug: /connectors
---

# 数据连接器总览

这一页不是参数大全，而是帮助第一次选择 SeaTunnel 连接器的用户先找到正确入口。建议先确认“从哪里读”“写到哪里”“是否需要 CDC 或特殊格式”，再进入具体连接器参数页。

## 先按任务目标选入口

| 你现在要做什么 | 先看这里                                                                                                      |
| --- |-----------------------------------------------------------------------------------------------------------|
| 从外部系统读取数据 | [数据来源连接器](./source-overview.md)                                                                           |
| 把数据写入目标系统 | [数据写入连接器](./sink-overview.md)                                                                             |
| 先找一条接近真实业务的链路示例 | [场景示例](../getting-started/recipes/overview.md)                                                            |
| 先理解连接器共有参数 | [来源端通用参数](./common-options/source-common-options.md) 和 [写入端通用参数](./common-options/sink-common-options.md) |
| 构建 CDC 链路 | [CDC 生产实战手册](./cdc-production-cookbook.md)                                                                |
| 排查插件安装或依赖冲突 | [连接器常见问题](./connector-faq.md) 和 [连接器依赖隔离加载机制](./connector-isolated-dependency.md)                        |

## 新用户推荐顺序

1. 先跑通一个本地任务，再回来选择真实连接器。
2. 先确定来源端和写入端，再判断是否需要额外的数据转换或格式处理。
3. 在复制示例参数前，先确认插件安装和第三方驱动是否齐全。
4. 只有在任务真的涉及多表、增量或恢复语义时，再深入 CDC 和容错细节。

## 选择连接器时重点关注什么

- 是否支持你正在使用的执行引擎
- 是否需要额外驱动或插件安装
- 是否支持批处理、流处理、CDC、精确一次等能力
- 参数名、默认值和示例是否与你的 SeaTunnel 版本一致

## 常用下一步

- [作业配置指南](../getting-started/job-configuration-guide.md)
- [场景示例](../getting-started/recipes/overview.md)
- [数据转换总览](../transforms/overview.md)
- [SeaTunnel 引擎快速开始](../getting-started/locally/quick-start-seatunnel-engine.md)
