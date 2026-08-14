---
sidebar_position: 16
---

# ClassLoader 深度清理模式

:::caution 警告

此功能目前处于实验阶段。ClassLoader 治理路线图的 Phase 3（完整的引用图清理与统一的 TCCL / JDBC driver 管理）尚未完成，因此深度清理路径在部分边界场景下可能无法达到预期效果。此处描述的属性名、JVM 参数与行为在 Phase 3 落地前可能变更。请仅在你理解下方权衡时使用，且在 Phase 3 完成前不要依赖它进行跨作业 JAR 共享场景。

:::

`SEATUNNEL_CLASSLOADER_DEEP_CLEAN` 是一个可选启用的 JVM 系统属性（默认 `false`），用于在 ClassLoader 释放时对其资源（JAR 文件句柄与内部 `URLClassPath` 缓存）进行更彻底的清理。适用于 ClassLoader 泄漏导致 Metaspace `OutOfMemoryError` 的长周期引擎场景。

## 用法（JDK 9 及以上）

通过设置 JVM 系统属性并添加两个必需的 `--add-opens` 参数来启用：

```bash
-DSEATUNNEL_CLASSLOADER_DEEP_CLEAN=true \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/jdk.internal.loader=ALL-UNNAMED
```

两个 `--add-opens` 参数是仅存在于 JDK 9 及以上的 JPMS launcher 选项。**在 JDK 8 上不要添加它们**——JDK 8 launcher 会拒绝这些参数，导致 JVM 启动失败。

两个 `--add-opens` 参数都是反射式缓存清理完整生效所必需的。若缺失，清理会优雅降级——`URLClassLoader.close()` 仍会释放底层 `JarFile` 文件描述符，但陈旧引用的清理会被跳过，并打印一条 `WARN` 日志。

## 在哪里设置

该属性在 `DefaultClassLoaderService` 内部读取，它运行在**服务端**进程中，因此必须添加到 master / worker 的 JVM 选项中。仅写入 `jvm_client_options` 不会生效。

- Hybrid 集群：`config/jvm_options`
- Separated 集群：`config/jvm_master_options` 与 `config/jvm_worker_options`

## JDK 8 注意事项

在 JDK 8 上不存在 protocol-scoped 的 `setDefaultUseCaches(String, boolean)` API。实现会退化为 JVM 全局的 `setDefaultUseCaches(false)`，这会对**所有** `URLConnection` 协议（http / file 等）设置 `useCaches=false`，而不仅限于 `jar`。这是已知的平台限制，与其他 JVM 泄漏防护工具一致。

在 JDK 8 上不需要 `--add-opens`（无模块系统，`URLClassPath` 经 `sun.misc` 可达），且**不能**添加——JDK 8 launcher 会拒绝 `--add-opens` 并导致 JVM 启动失败。
