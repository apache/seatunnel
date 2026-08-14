---
sidebar_position: 16
---

# ClassLoader Deep Clean Mode

:::caution warn

This feature is currently in an experimental stage. Phase 3 of the ClassLoader governance roadmap (full reference-graph cleanup and unified TCCL / JDBC driver management) is not yet complete, so the deep-clean path may not behave as expected in all edge cases. The property name, flags, and behavior described here are subject to change before Phase 3 lands. Use it only if you understand the trade-offs below, and do not rely on it for cross-job JAR sharing scenarios yet.

:::

`SEATUNNEL_CLASSLOADER_DEEP_CLEAN` is an opt-in JVM system property (default `false`) that enables aggressive cleanup of ClassLoader resources — JAR file handles and internal `URLClassPath` caches — when a ClassLoader is released. It is intended for long-running engine scenarios where ClassLoader leaks cause Metaspace `OutOfMemoryError`.

## Usage

Enable by setting the JVM system property together with both required `--add-opens` flags:

```bash
-DSEATUNNEL_CLASSLOADER_DEEP_CLEAN=true \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/jdk.internal.loader=ALL-UNNAMED
```

Both `--add-opens` flags are required for the reflective cache-clearing to fully succeed. Without them the cleanup degrades gracefully — `URLClassLoader.close()` still releases the underlying `JarFile` file descriptors, but the stale-reference cleanup is skipped and a `WARN` is logged.

## JDK 8 Caveat

On JDK 8 there is no protocol-scoped `setDefaultUseCaches(String, boolean)` API. The implementation falls back to the JVM-global `setDefaultUseCaches(false)`, which sets `useCaches=false` for **all** `URLConnection` protocols (http / file / etc.), not only `jar`. This is a known platform limitation shared with other JVM leak-prevention tools.
