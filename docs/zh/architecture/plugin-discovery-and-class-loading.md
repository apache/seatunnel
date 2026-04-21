---
title: Plugin Discovery and Class Loading
---

# 插件发现与类加载

## 为什么需要这篇文档

SeaTunnel 现有文档已经说明了 connector 怎么配置、二进制包里的依赖隔离目录怎么放，但还缺一个“从作业配置到插件实例”的运行时总览。

这篇文档解释的就是这条路径：

- 配置里的插件名是如何变成一个 factory 实现的
- 插件元数据是如何被发现的
- connector jar 和依赖 jar 是如何被定位到的
- 为什么类加载隔离尤其在 Zeta 里很关键

## SeaTunnel 需要解决的问题

SeaTunnel 要支持大量 source、sink、transform、format 插件，运行时必须可靠地回答下面这些问题：

- 用户配置对应的是哪个插件实现
- 这个插件的 jar 和隔离依赖目录在哪里
- 如何在不污染整个进程 classpath 的前提下加载它们
- 如何把插件元数据暴露给 CLI、REST API 和 Web UI

如果没有插件发现与类加载这一层，connector 数量一多，很快就会出现依赖冲突和启动脆弱性问题。

## 运行时路径总览

一个插件从配置到执行，通常会经过下面这条链路：

```text
作业配置
  -> 解析插件名
  -> 发现 factory
  -> 校验 options
  -> 定位 connector jar 与隔离依赖
  -> 创建插件 classloader
  -> 实例化 source / sink / transform
  -> 在引擎运行时中执行
```

每一步的作用都不同：

- discovery 负责找到正确实现
- validation 负责提前拦截错误配置
- jar resolution 决定哪些代码真正可用
- classloader isolation 负责降低依赖冲突

## 插件发现模型

### 插件标识

运行时 SeaTunnel 会用一个逻辑标识来区分插件，通常会包含：

- engine type
- plugin type，例如 source 或 sink
- plugin name

这个标识会被用于：

- 配置解析
- 插件查找
- option 元数据暴露
- 日志与诊断输出

### Factory 发现

大多数用户可见插件都是通过 factory 接口创建的。实际运行时，SeaTunnel 主要依赖 Java SPI 和 factory discovery 工具，去加载 `META-INF/services` 下声明的实现。

典型例子包括：

- `TableSourceFactory`
- `TableSinkFactory`
- transform factory
- 某些 catalog / format factory

这也是为什么 connector 开发文档里会要求补 factory 注册与 service 元数据。

相关文档：

- [Source Connector 开发指南](../developer/source-connector-development.md)
- [Sink Connector 开发指南](../developer/sink-connector-development.md)

## Jar 定位与打包

SeaTunnel 会把插件实现 jar 与 connector 专属的第三方依赖分开管理。

典型布局如下：

```text
SEATUNNEL_HOME/
  connectors/
    connector-jdbc-<version>.jar
  plugins/
    connector-jdbc/
      dependency-a.jar
      dependency-b.jar
```

插件名与依赖目录之间的映射关系由 `plugin-mapping.properties` 管理。

这种设计带来两个直接收益：

- connector 实现 jar 可以集中分发
- connector 专属依赖可以彼此隔离

相关文档：

- [Connector 依赖隔离加载机制](../connectors/connector-isolated-dependency.md)

## 类加载的实际意义

### 为什么需要隔离

不同 connector 很可能依赖同一个第三方库的不同版本。如果所有 jar 都进入一个扁平 classpath，那么一个版本冲突就可能影响同一作业里的其他 connector。

类加载隔离主要在这三处体现价值：

- connector 启动
- task 部署
- 多 connector 的长时间运行作业

### Zeta 与 Flink / Spark 的差异

目前最重要的实践差异是：SeaTunnel Engine（Zeta）对 connector 依赖隔离支持更强。现有隔离依赖文档已经明确提到，Spark 和 Flink 在运行期 classpath 共享更紧，因此混合版本场景风险更高。

这在下面几类问题里尤其关键：

- 新增一个依赖树很重的 connector
- 排查 `ClassNotFoundException` 或 `NoSuchMethodError`
- 在分布式集群里打包和部署作业

## 元数据发现与 Option 暴露

插件发现不只是为了实例化运行时对象，它同时也支撑了元数据能力，例如通过 REST API 或 Web UI 暴露 connector 的 option 信息。

因此插件系统不仅要能发现 factory，还要能拿到这类元数据：

- 支持的 plugin identifier
- `OptionRule`
- required / optional 字段
- 参数分组与校验语义

相关文档：

- [配置与 Option 系统](./configuration-and-option-system.md)
- [REST API and Web UI](../engines/zeta/rest-api-and-web-ui.md)

## 常见失败模式

插件加载失败时，报错症状通常已经能说明是哪一层出了问题。

### Discovery 失败

典型表现：

- plugin not found
- 文档里有这个 connector，但运行时识别不到

常见原因：

- connector jar 缺失
- SPI 注册缺失
- plugin identifier 不匹配

### Validation 失败

典型表现：

- 作业提交前就失败
- REST/UI 能看到元数据，但配置校验不过

常见原因：

- 必填参数缺失
- 互斥参数同时出现
- 文档里的 option 名与 factory 定义不一致

### Classpath / ClassLoader 失败

典型表现：

- `ClassNotFoundException`
- `NoClassDefFoundError`
- `NoSuchMethodError`
- 只在某个引擎上出现版本冲突

常见原因：

- 依赖 jar 没放进 plugin 目录
- connector 打包时带入了不兼容依赖
- 集群节点之间的插件目录不一致

## 运维排查清单

当插件无法正确加载时，建议按这个顺序检查：

1. connector jar 是否存在于二进制包中
2. `${SEATUNNEL_HOME}/plugins/` 下的依赖目录是否正确
3. `plugin-mapping.properties` 是否映射到了预期目录
4. 集群各节点的插件布局是否一致
5. 作业配置里的插件名是否与 factory identifier 完全一致
6. 问题是否只在 Flink 或 Spark 发生，而 Zeta 正常

## 开发者检查清单

新增一个插件时，在怀疑引擎内部逻辑之前，建议先确认这些点：

- factory 是否正确注册
- factory identifier 是否与文档示例一致
- `OptionRule` 是否覆盖 required / optional / exclusive 语义
- connector 是否已经加入打包与 plugin mapping
- 隔离依赖是否放在了预期的插件目录下

## 代码入口

建议从这些类开始看：

- `seatunnel-api/src/main/java/org/apache/seatunnel/api/table/factory/FactoryUtil.java`
- `seatunnel-engine/seatunnel-engine-common/src/main/java/org/apache/seatunnel/engine/common/utils/FactoryUtil.java`
- `seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/classloader/ClassLoaderService.java`
- `seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/classloader/DefaultClassLoaderService.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/rest/service/OptionRulesService.java`

## 推荐阅读顺序

1. 先读本页，建立运行时地图
2. 再读 [Connector 依赖隔离加载机制](../connectors/connector-isolated-dependency.md)
3. 再读 [配置与 Option 系统](./configuration-and-option-system.md)
4. 然后按需进入 [Source Connector 开发指南](../developer/source-connector-development.md) 或 [Sink Connector 开发指南](../developer/sink-connector-development.md)
