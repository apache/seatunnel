# 贡献 Connector-V2 插件

如果你想贡献 Connector-V2，建议优先沿着站内文档入口进入，再结合仓库中的 Connector-V2 指南查看模块级细节。

## 编写 Connector FAQ 的要求

新增或更新 connector FAQ 时，应把 FAQ 当作“导航层”，而不是另一套独立事实源。

- 精确的配置项名称、默认值和完整行为说明，应保留在 connector 的 option 表和详细章节中。
- FAQ 答案优先写成“短答 + 指向现有章节”，不要在 FAQ 中再复制一套完整配置矩阵。
- 只要 FAQ 提到 connector 配置项，合入前就必须对照当前 connector 文档和源码核对拼写与语义。
- 如果某个结论依赖 passthrough 属性或上游数据库 / 消息系统本身的行为，必须明确标注，不能把它包装成 SeaTunnel 标准 connector 配置项。
- 英文和中文 FAQ 的覆盖范围应保持一致。

- [贡献路径](./contribution-path.md)
- [开发自己的 Connector](./how-to-create-your-connector.md)
- [Source Connector 开发指南](./source-connector-development.md)
- [Sink Connector 开发指南](./sink-connector-development.md)
- [搭建开发环境](./setup.md)
- 外部仓库指南：[Connector-v2 贡献指南](../../../seatunnel-connectors-v2/README.zh.md)
