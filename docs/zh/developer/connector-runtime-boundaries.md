# 连接器运行时边界

本页说明 connector runtime API、option rule、catalog 和 metadata provider 之间的边界。适用于
开发连接器，或把 SeaTunnel 接入外部元数据系统的开发者。

## Connector Source/Sink SPI 是运行时数据通路

`TableSourceFactory` 和 `TableSinkFactory` 负责创建真正读写数据的 runtime Source/Sink 对象。
连接器运行时代码负责：

- 创建 Source 或 Sink 实例；
- 校验并读取连接器参数；
- 生产或消费 `SeaTunnelRow`；
- 在连接器支持时保留 schema、主键、表路径、row kind 等元信息。

不要把某个连接器特有的读写逻辑放进 engine、core 或 metadata provider 代码中。

## OptionRule 是配置契约

每个连接器 factory 都会暴露 `optionRule()`。SeaTunnel 使用它校验用户配置，也让 Web UI 或
plugin discovery 代码知道哪些参数必填、哪些参数可选。

`OptionRule` 应描述连接器的配置表面。它不应该打开网络连接、发现远端 schema，也不应该执行
运行时副作用。

新增或修改用户可见参数时：

- 使用 `Option` 定义参数；
- 只有在连接器确实有稳定默认值时才设置 default；
- 保持既有 option 名称向后兼容；
- 同步更新英文和中文文档。

## CatalogFactory 用于 catalog 元数据操作

`CatalogFactory` 创建 `Catalog`，用于列出 database、列出 table、读取 table schema 等元数据
操作。一个连接器可以同时提供 runtime source/sink factory 和 catalog factory，但它们是不同
契约。

当操作对象是元数据时，使用 catalog 代码；当操作对象是数据记录读写时，使用 source/sink
runtime 代码。

## MetadataProvider 用于外部 datasource 解析

`MetadataProvider` 是面向外部元数据服务的独立 SPI。它把 `metadata_datasource_id` 或外部表 ID
解析为 SeaTunnel 连接器配置或 table schema。

Provider 在连接器创建之前工作。它应该负责把外部元数据映射为 SeaTunnel config；不应该替代
connector source/sink 实现，也不应该包含连接器运行时读写逻辑。

## 提交连接器 PR 前的实用边界检查

- 连接器 factory 是否通过 `OptionRule` 暴露了全部用户可见参数？
- option 名称、默认值和文档是否一致？
- 如果连接器支持 catalog 操作，是否通过 `CatalogFactory` 注册，而不是隐藏在 source/sink
  runtime 代码里？
- 如果使用外部元数据，`MetadataProvider` 是否只解析 config/schema，把运行时读写留给连接器？
- source、sink、catalog、SPI 注册、plugin mapping、文档和测试覆盖的是不是同一个功能范围？
