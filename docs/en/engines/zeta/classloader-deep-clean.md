---
sidebar_position: 16
---

# ClassLoader Deep Clean Mode

:::caution warn

This feature is currently in an experimental stage. Phase 3 of the ClassLoader governance roadmap (full reference-graph cleanup and unified TCCL / JDBC driver management) is not yet complete, so the deep-clean path may not behave as expected in all edge cases. The property name, flags, and behavior described here are subject to change before Phase 3 lands. Use it only if you understand the trade-offs below, and do not rely on it for cross-job JAR sharing scenarios yet.

:::

`SEATUNNEL_CLASSLOADER_DEEP_CLEAN` is an opt-in JVM system property (default `false`) that enables aggressive cleanup of ClassLoader resources — JAR file handles and internal `URLClassPath` caches — when a ClassLoader is released. It is intended for long-running engine scenarios where ClassLoader leaks cause Metaspace `OutOfMemoryError`.

## Usage (JDK 9 and above)

Enable by setting the JVM system property together with both required `--add-opens` flags:

```bash
-DSEATUNNEL_CLASSLOADER_DEEP_CLEAN=true \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/jdk.internal.loader=ALL-UNNAMED
```

The two `--add-opens` flags are JPMS launcher options that exist only on JDK 9 and above. Do not add them on JDK 8 — the JDK 8 launcher rejects them and the JVM will fail to start.

Both `--add-opens` flags are required for the reflective cache-clearing to fully succeed. Without them the cleanup degrades gracefully — `URLClassLoader.close()` still releases the underlying `JarFile` file descriptors, but the stale-reference cleanup is skipped and a `WARN` is logged.

## Where to Set It

The property is read inside `DefaultClassLoaderService`, which runs in the **server** process, so it must be added to the master / worker JVM options. Setting it only in `jvm_client_options` has no effect.

- Hybrid clusters: `config/jvm_options`
- Separated clusters: `config/jvm_master_options` and `config/jvm_worker_options`

## JDK 8 Caveat

On JDK 8 there is no protocol-scoped `setDefaultUseCaches(String, boolean)` API. The implementation falls back to the JVM-global `setDefaultUseCaches(false)`, which sets `useCaches=false` for **all** `URLConnection` protocols (http / file / etc.), not only `jar`. This is a known platform limitation shared with other JVM leak-prevention tools.

On JDK 8 no `--add-opens` flags are needed (no module system; `URLClassPath` is reachable via `sun.misc`), and they must not be added — the JDK 8 launcher rejects `--add-opens` and the JVM fails to start.
