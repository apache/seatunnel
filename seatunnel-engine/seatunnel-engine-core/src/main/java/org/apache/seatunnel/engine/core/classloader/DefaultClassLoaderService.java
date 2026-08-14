/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.core.classloader;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.engine.common.exception.ClassLoaderErrorCode;
import org.apache.seatunnel.engine.common.exception.ClassLoaderException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DefaultClassLoaderService implements ClassLoaderService {
    private final boolean cacheMode;
    private final Map<Long, Map<String, ClassLoader>> classLoaderCache;
    private final Map<Long, Map<String, AtomicInteger>> classLoaderReferenceCount;
    private final NodeEngine nodeEngine;
    public static final String SKIP_CHECK_JAR = "CLASSLOADER_SERVICE_SKIP_CHECK_JAR";
    public static final String ENABLE_DEEP_CLEAN = "SEATUNNEL_CLASSLOADER_DEEP_CLEAN";
    private static final AtomicBoolean JAR_CACHE_DISABLED = new AtomicBoolean(false);

    private final boolean deepCleanEnabled;

    public DefaultClassLoaderService(boolean cacheMode, NodeEngine nodeEngine) {
        this.cacheMode = cacheMode;
        this.nodeEngine = nodeEngine;
        // Read once into an instance field so each service instance carries its own deep-clean
        // state. The previous static-flag-with-unconditional-set design let a second instance
        // silently flip the first instance's behavior; an instance field removes that hazard.
        this.deepCleanEnabled = readDeepCleanEnabled();
        classLoaderCache = new ConcurrentHashMap<>();
        classLoaderReferenceCount = new ConcurrentHashMap<>();
        // Default path: never touch URLConnection cache defaults. Only opt-in deep-clean
        // instances perform the JAR cache disable, which preserves prior default behavior.
        if (deepCleanEnabled) {
            disableJarUrlCache();
            log.info("Deep clean mode enabled");
        }
        log.info("start classloader service{}", cacheMode ? " with cache mode" : "");
    }

    // Accepts both a system property (-D...) and an environment variable of the same name;
    // the property wins when both are present. Accepting the env form prevents silent no-ops
    // for operators who export the flag instead of using -D (the sibling SKIP_CHECK_JAR
    // constant is env-only, which is easy to confuse with this property).
    private static boolean readDeepCleanEnabled() {
        return resolveDeepCleanEnabled(
                System.getProperty(ENABLE_DEEP_CLEAN),
                System.getenv().getOrDefault(ENABLE_DEEP_CLEAN, "false"));
    }

    @VisibleForTesting
    static boolean resolveDeepCleanEnabled(String propertyValue, String envValue) {
        if (propertyValue != null) {
            return Boolean.parseBoolean(propertyValue);
        }
        return Boolean.parseBoolean(envValue == null ? "false" : envValue);
    }

    private void disableJarUrlCache() {
        // URLConnection.setDefaultUseCaches is JVM-global and idempotent; the CAS guard makes
        // the disable attempt run at most once per JVM regardless of how many deep-clean
        // services are constructed. If the disable itself fails (caught below), the flag stays
        // set so no later construction retries — the WARN log is the only signal of that outcome.
        if (!JAR_CACHE_DISABLED.compareAndSet(false, true)) {
            return;
        }
        try {
            // JDK 9+ added a protocol-scoped overload: setDefaultUseCaches(String, boolean).
            // Use it when available so non-JAR protocols (http/file/...) keep their defaults.
            Method protocolScoped =
                    URLConnection.class.getMethod(
                            "setDefaultUseCaches", String.class, boolean.class);
            protocolScoped.invoke(null, "jar", false);
            log.info("Disabled JAR URL connection cache (protocol-scoped, JDK 9+)");
        } catch (NoSuchMethodException e) {
            // JDK 8: only the JVM-global 1-arg API exists. Side effect: every URLConnection
            // protocol inherits useCaches=false. Mirrors Tomcat's
            // JreMemoryLeakPreventionListener, which faces the same platform limitation.
            try {
                URLConnection connection = new URL("jar:file://dummy.jar!/").openConnection();
                connection.setDefaultUseCaches(false);
                log.warn(
                        "Disabled JAR URL cache via JVM-global toggle (JDK 8 has no "
                                + "protocol-scoped API). Side effect: default useCaches=false "
                                + "for ALL URLConnection protocols (http/file/etc.) for this JVM. "
                                + "See docs for trade-offs.");
            } catch (Exception ex) {
                log.warn("Failed to disable JAR URL connection cache: {}", ex.getMessage());
            }
        } catch (Exception e) {
            log.warn("Failed to disable JAR URL connection cache: {}", e.getMessage());
        }
    }

    @SneakyThrows
    @Override
    public synchronized ClassLoader getClassLoader(long jobId, Collection<URL> jars) {
        log.debug("Get classloader for job {} with jars {}", jobId, jars);
        if (cacheMode) {
            // with cache mode, all jobs share the same classloader if the jars are the same
            jobId = 1L;
        }
        if (!classLoaderCache.containsKey(jobId)) {
            classLoaderCache.put(jobId, new ConcurrentHashMap<>());
            classLoaderReferenceCount.put(jobId, new ConcurrentHashMap<>());
        }
        Map<String, ClassLoader> classLoaderMap = classLoaderCache.get(jobId);
        String key = covertJarsToKey(jars);
        if (classLoaderMap.containsKey(key)) {
            classLoaderReferenceCount.get(jobId).get(key).incrementAndGet();
            return classLoaderMap.get(key);
        } else {
            if (Objects.nonNull(nodeEngine)
                    && !Boolean.parseBoolean(
                            System.getenv().getOrDefault(SKIP_CHECK_JAR, "false"))) {
                for (URL jar : jars) {
                    File file = new File(jar.toURI().getPath());
                    if (!file.exists()) {
                        String host =
                                ((NodeEngineImpl) nodeEngine).getNode().getThisAddress().getHost();
                        throw new ClassLoaderException(
                                ClassLoaderErrorCode.NOT_FOUND_JAR,
                                "The jar file "
                                        + jar
                                        + " can not be found in node "
                                        + host
                                        + ", please ensure that the deployment paths of SeaTunnel on different nodes are consistent.");
                    }
                }
            } else {
                log.debug("Run the test class without file checking");
            }
            ClassLoader classLoader = new SeaTunnelChildFirstClassLoader(jars);
            log.info("Create classloader for job {} with jars {}", jobId, jars);
            classLoaderMap.put(key, classLoader);
            classLoaderReferenceCount.get(jobId).put(key, new AtomicInteger(1));
            return classLoader;
        }
    }

    @Override
    public synchronized void releaseClassLoader(long jobId, Collection<URL> jars) {
        log.debug("Release classloader for job {} with jars {}", jobId, jars);
        if (cacheMode) {
            // with cache mode, all jobs share the same classloader if the jars are the same
            jobId = 1L;
        }
        if (!classLoaderCache.containsKey(jobId)) {
            return;
        }
        Map<String, ClassLoader> classLoaderMap = classLoaderCache.get(jobId);
        String key = covertJarsToKey(jars);
        if (!classLoaderMap.containsKey(key)) {
            return;
        }
        int referenceCount = classLoaderReferenceCount.get(jobId).get(key).decrementAndGet();
        log.debug("Reference count for job {} with jars {} is {}", jobId, jars, referenceCount);
        if (cacheMode) {
            return;
        }
        if (referenceCount == 0) {
            ClassLoader classLoader = classLoaderMap.remove(key);
            log.info("Release classloader for job {} with jars {}", jobId, jars);
            classLoaderReferenceCount.get(jobId).remove(key);
            recycleClassLoaderFromThread(classLoader);
            closeClassLoader(classLoader);
        }
        if (classLoaderMap.isEmpty()) {
            classLoaderCache.remove(jobId);
            classLoaderReferenceCount.remove(jobId);
        }
    }

    /**
     * [Phase 1 Compromise] Forcibly removes the target ClassLoader from the ContextClassLoader
     * (TCCL) of all live threads.
     *
     * <p>Many third-party connectors (e.g., JDBC drivers, Hadoop RPC clients) mutate the TCCL
     * during execution but fail to restore it in a finally block. If left uncleared, these pooled
     * threads will hold strong references to the {@link SeaTunnelChildFirstClassLoader}, preventing
     * GC and ultimately causing severe Metaspace OutOfMemory (OOM) errors.
     *
     * <p>Note: We restore the TCCL to the system/application classloader (systemLoader) instead of
     * setting it to 'null'. Setting TCCL to 'null' breaks frameworks that heavily rely on it for
     * SPI discovery or static resource loading (e.g., Hazelcast, Jetty REST APIs), which would
     * otherwise result in 404 Not Found errors during E2E integration tests.
     */
    private static void recycleClassLoaderFromThread(ClassLoader classLoader) {
        // Acquire a safe fallback ClassLoader (usually the SystemClassLoader)
        final ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getContextClassLoader() == classLoader)
                .forEach(
                        thread -> {
                            log.info("recycle classloader for thread {}", thread.getName());
                            // Restore to the default loader instead of null
                            thread.setContextClassLoader(systemLoader);
                        });
    }

    private String covertJarsToKey(Collection<URL> jars) {
        return jars.stream().map(URL::toString).sorted().reduce((a, b) -> a + b).orElse("");
    }

    /** Only for test */
    @VisibleForTesting
    public Optional<ClassLoader> queryClassLoaderById(long jobId, Collection<URL> jars) {
        if (cacheMode) {
            // with cache mode, all jobs share the same classloader if the jars are the same
            jobId = 1L;
        }
        if (!classLoaderCache.containsKey(jobId)) {
            return Optional.empty();
        }
        Map<String, ClassLoader> classLoaderMap = classLoaderCache.get(jobId);
        String key = covertJarsToKey(jars);
        if (!classLoaderMap.containsKey(key)) {
            return Optional.empty();
        }
        return Optional.of(classLoaderMap.get(key));
    }

    /** Only for test */
    @VisibleForTesting
    public int queryClassLoaderReferenceCount(long jobId, Collection<URL> jars) {
        if (cacheMode) {
            // with cache mode, all jobs share the same classloader if the jars are the same
            jobId = 1L;
        }
        if (!classLoaderCache.containsKey(jobId)) {
            return 0;
        }
        Map<String, AtomicInteger> classLoaderMap = classLoaderReferenceCount.get(jobId);
        String key = covertJarsToKey(jars);
        if (!classLoaderMap.containsKey(key)) {
            return 0;
        }
        return classLoaderMap.get(key).get();
    }

    /** Only for test */
    @VisibleForTesting
    public int queryClassLoaderCount() {
        AtomicInteger count = new AtomicInteger();
        classLoaderCache.values().forEach(map -> count.addAndGet(map.size()));
        return count.get();
    }

    @Override
    public synchronized void close() {
        log.info("close classloader service");
        closeAllClassLoaders();
        classLoaderCache.clear();
        classLoaderReferenceCount.clear();
    }

    private void closeClassLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }

        // [Phase 1 Compromise]
        // Currently, physical closure (URLClassLoader.close) and deep cache cleanups are strictly
        // guarded by the deepCleanEnabled instance flag.
        //
        // Reason: Many connectors currently leak their ClassLoader references into the global TCCL
        // or static caches. In a shared JVM test environment (like GitHub CI), explicitly closing
        // the Jar handlers here will cause subsequent ITs to fail with ZipException or NPE.
        // Once the global TCCL isolation is fully implemented in Phase 3, this physical closure
        // can be safely moved outside the flag as the default behavior.
        if (deepCleanEnabled) {
            closeUrlClassLoader(classLoader);
            boolean cacheCleared = clearUrlClassPathCache(classLoader);
            if (cacheCleared) {
                log.info("Deep clean for ClassLoader completed successfully.");
            } else {
                log.info("Deep clean for ClassLoader completed with degraded cache cleanup.");
            }
        }
    }

    private void closeUrlClassLoader(ClassLoader classLoader) {
        if (classLoader instanceof URLClassLoader) {
            try {
                ((URLClassLoader) classLoader).close();
                log.info("Successfully closed URLClassLoader: {}", classLoader);
            } catch (IOException e) {
                log.warn(
                        "Failed to close URLClassLoader: {}, error: {}",
                        classLoader,
                        e.getMessage());
            }
        }
    }

    private void closeAllClassLoaders() {
        for (Map.Entry<Long, Map<String, ClassLoader>> jobEntry : classLoaderCache.entrySet()) {
            Map<String, ClassLoader> loaderMap = jobEntry.getValue();
            for (Map.Entry<String, ClassLoader> loaderEntry : loaderMap.entrySet()) {
                closeClassLoader(loaderEntry.getValue());
            }
        }
    }

    private boolean clearUrlClassPathCache(ClassLoader classLoader) {
        if (!(classLoader instanceof URLClassLoader)) {
            return false;
        }
        try {
            clearUrlClassPathCacheReflectively((URLClassLoader) classLoader);
            log.info("Cleared URLClassPath cache for: {}", classLoader);
            return true;
        } catch (Exception e) {
            // Configuration/Initialization errors -> Elevated to WARN
            // URLClassPath lives in jdk.internal.loader (not java.net) on JDK 9+, so both
            // --add-opens flags are required for the reflective cache-clearing to fully succeed.
            // Failure here degrades gracefully: URLClassLoader.close() (called above) already
            // released the underlying JarFile fd handles; only the stale-reference cleanup is
            // skipped.
            log.warn(
                    "Failed to clear URLClassPath cache due to reflection restrictions. "
                            + "Please add BOTH JVM options:\n"
                            + "  --add-opens java.base/java.net=ALL-UNNAMED\n"
                            + "  --add-opens java.base/jdk.internal.loader=ALL-UNNAMED",
                    e);
            return false;
        }
    }

    /**
     * Reflectively clears {@code URLClassPath}'s internal {@code loaders} / {@code lmap} caches
     * that hold stale {@code JarFile} references after {@link URLClassLoader#close()}.
     *
     * <p>Extracted as a protected hook so tests can simulate JDK 9+ {@code
     * InaccessibleObjectException} (missing {@code --add-opens}) and assert the caller degrades
     * gracefully rather than propagating.
     *
     * @param classLoader the classloader whose URLClassPath cache is to be cleared
     * @throws Exception if reflective access fails (caller is responsible for catching)
     */
    @VisibleForTesting
    protected void clearUrlClassPathCacheReflectively(URLClassLoader classLoader) throws Exception {
        Field ucpField = URLClassLoader.class.getDeclaredField("ucp");
        ucpField.setAccessible(true);
        Object ucp = ucpField.get(classLoader);
        if (ucp == null) {
            return;
        }
        Field loadersField = ucp.getClass().getDeclaredField("loaders");
        loadersField.setAccessible(true);
        Object loaders = loadersField.get(ucp);
        // JDK 9+ uses an ArrayList while JDK 8 uses a Vector; matching against List covers both
        // so the stale JarFile cleanup also runs on JDK 8 instead of being silently skipped.
        if (loaders instanceof List) {
            List<?> loadersList = (List<?>) loaders;
            for (Object loader : loadersList) {
                closeJarLoader(loader);
            }
            loadersList.clear();
        }
        Field lmapField = ucp.getClass().getDeclaredField("lmap");
        lmapField.setAccessible(true);
        Object lmap = lmapField.get(ucp);
        if (lmap instanceof Map) {
            ((Map<?, ?>) lmap).clear();
        }
    }

    private void closeJarLoader(Object loader) {
        try {
            Field jarFileField = loader.getClass().getDeclaredField("jar");
            jarFileField.setAccessible(true);
            Object jarFile = jarFileField.get(loader);
            // JarFile extends ZipFile which implements Closeable since Java 7; no need to
            // reflect for a declared close() method (it is inherited from ZipFile, so
            // getDeclaredMethod("close") would always throw NoSuchMethodException).
            if (jarFile instanceof Closeable) {
                ((Closeable) jarFile).close();
                log.info("Closed JarFile: {}", jarFile);
            }
        } catch (Exception e) {
            // Per-resource cleanup failures -> Keep at DEBUG
            log.debug("Failed to close JarLoader: {}", e.getMessage());
        }
    }
}
