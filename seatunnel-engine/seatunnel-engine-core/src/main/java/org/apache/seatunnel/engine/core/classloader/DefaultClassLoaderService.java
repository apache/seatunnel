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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;

@Slf4j
public class DefaultClassLoaderService implements ClassLoaderService {
    private final boolean cacheMode;
    private final Map<Long, Map<String, ClassLoader>> classLoaderCache;
    private final Map<Long, Map<String, AtomicInteger>> classLoaderReferenceCount;
    private final NodeEngine nodeEngine;
    public static final String SKIP_CHECK_JAR = "CLASSLOADER_SERVICE_SKIP_CHECK_JAR";
    public static final String ENABLE_DEEP_CLEAN = "SEATUNNEL_CLASSLOADER_DEEP_CLEAN";
    private static final AtomicBoolean JAR_CACHE_DISABLED = new AtomicBoolean(false);
    private static final AtomicBoolean DEEP_CLEAN_ENABLED = new AtomicBoolean(false);

    public DefaultClassLoaderService(boolean cacheMode, NodeEngine nodeEngine) {
        this.cacheMode = cacheMode;
        this.nodeEngine = nodeEngine;
        classLoaderCache = new ConcurrentHashMap<>();
        classLoaderReferenceCount = new ConcurrentHashMap<>();
        disableJarUrlCache();
        log.info("start classloader service {}", cacheMode ? " with cache mode" : "");
    }

    private void disableJarUrlCache() {
        if (JAR_CACHE_DISABLED.compareAndSet(false, true)) {
            try {
                URLConnection connection = new URL("jar:file://dummy.jar!/").openConnection();
                connection.setDefaultUseCaches(false);
                log.info("Disabled JAR URL connection cache");
            } catch (Exception e) {
                log.warn("Failed to disable JAR URL connection cache: {}", e.getMessage());
            }
        }
        boolean deepClean = Boolean.parseBoolean(System.getProperty(ENABLE_DEEP_CLEAN, "false"));
        DEEP_CLEAN_ENABLED.set(deepClean);
        if (deepClean) {
            log.info("Deep clean mode enabled (requires --add-opens JVM options)");
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

    private static void recycleClassLoaderFromThread(ClassLoader classLoader) {
        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getContextClassLoader() == classLoader)
                .forEach(
                        thread -> {
                            log.info("recycle classloader for thread {}", thread.getName());
                            thread.setContextClassLoader(null);
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
    public void close() {
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
        // guarded by the DEEP_CLEAN_ENABLED flag.
        //
        // Reason: Many connectors currently leak their ClassLoader references into the global TCCL
        // or static caches. In a shared JVM test environment (like GitHub CI), explicitly closing
        // the Jar handlers here will cause subsequent ITs to fail with ZipException or NPE.
        // Once the global TCCL isolation is fully implemented in Phase 3, this physical closure
        // can be safely moved outside the flag as the default behavior.
        if (DEEP_CLEAN_ENABLED.get()) {
            closeUrlClassLoader(classLoader);
            clearUrlClassPathCache(classLoader);
            clearJarFileFactoryCache(classLoader);

            // Success execution -> Elevated to INFO
            log.info("Deep clean for ClassLoader completed successfully.");
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
            } catch (NoSuchMethodError e) {
                log.debug("URLClassLoader.close() not available (Java < 7)");
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

    private void clearUrlClassPathCache(ClassLoader classLoader) {
        if (!(classLoader instanceof URLClassLoader)) {
            return;
        }
        try {
            Field ucpField = URLClassLoader.class.getDeclaredField("ucp");
            ucpField.setAccessible(true);
            Object ucp = ucpField.get(classLoader);
            if (ucp == null) {
                return;
            }
            Field loadersField = ucp.getClass().getDeclaredField("loaders");
            loadersField.setAccessible(true);
            Object loaders = loadersField.get(ucp);
            if (loaders instanceof ArrayList) {
                ArrayList<?> loadersList = (ArrayList<?>) loaders;
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
            log.info("Cleared URLClassPath cache for: {}", classLoader);
        } catch (Exception e) {
            // Configuration/Initialization errors -> Elevated to WARN
            log.warn(
                    "Failed to clear URLClassPath cache due to reflection restrictions. Please add '--add-opens java.base/java.net=ALL-UNNAMED' to JVM options.",
                    e);
        }
    }

    private void closeJarLoader(Object loader) {
        try {
            Field jarFileField = loader.getClass().getDeclaredField("jar");
            jarFileField.setAccessible(true);
            Object jarFile = jarFileField.get(loader);
            if (jarFile != null) {
                Method closeMethod = jarFile.getClass().getDeclaredMethod("close");
                closeMethod.setAccessible(true);
                closeMethod.invoke(jarFile);
                log.info("Closed JarFile: {}", jarFile);
            }
        } catch (NoSuchFieldException e) {
            try {
                Method closeMethod = loader.getClass().getDeclaredMethod("close");
                closeMethod.setAccessible(true);
                closeMethod.invoke(loader);
            } catch (Exception ex) {
                // Empty catch block fixed -> Keep at DEBUG to avoid spamming
                log.debug("Failed to invoke close() on inner jar loader: {}", ex.getMessage());
            }
        } catch (Exception e) {
            // Per-resource cleanup failures -> Keep at DEBUG
            log.debug("Failed to close JarLoader: {}", e.getMessage());
        }
    }

    private void clearJarFileFactoryCache(ClassLoader classLoader) {
        if (!(classLoader instanceof URLClassLoader)) {
            return;
        }
        try {
            Set<String> targetJarPaths = new HashSet<>();
            for (URL url : ((URLClassLoader) classLoader).getURLs()) {
                try {
                    String protocol = url.getProtocol();
                    if ("file".equalsIgnoreCase(protocol)) {
                        targetJarPaths.add(new File(url.toURI()).getCanonicalPath());
                    } else if ("jar".equalsIgnoreCase(protocol)) {
                        String fileUrlString = url.getFile();
                        if (fileUrlString.startsWith("file:")) {
                            int bangIndex = fileUrlString.indexOf("!");
                            if (bangIndex > 0) {
                                fileUrlString = fileUrlString.substring(0, bangIndex);
                            }
                            targetJarPaths.add(
                                    new File(new URL(fileUrlString).toURI()).getCanonicalPath());
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to parse URL for deep clean: {}", url, e);
                }
            }

            if (targetJarPaths.isEmpty()) {
                return;
            }

            Class<?> jarFileFactoryClass = Class.forName("sun.net.www.protocol.jar.JarFileFactory");
            Field fileCacheField = jarFileFactoryClass.getDeclaredField("fileCache");
            fileCacheField.setAccessible(true);
            Map<?, ?> fileCache = (Map<?, ?>) fileCacheField.get(null);

            Field urlCacheField = jarFileFactoryClass.getDeclaredField("urlCache");
            urlCacheField.setAccessible(true);
            Map<?, ?> urlCache = (Map<?, ?>) urlCacheField.get(null);

            synchronized (jarFileFactoryClass) {
                if (fileCache != null) {
                    Iterator<? extends Map.Entry<?, ?>> iterator = fileCache.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<?, ?> entry = iterator.next();
                        Object value = entry.getValue();
                        if (value instanceof JarFile) {
                            JarFile jarFile = (JarFile) value;
                            if (targetJarPaths.contains(jarFile.getName())) {
                                try {
                                    jarFile.close();
                                } catch (IOException e) {
                                    log.debug("Failed to close JarFile: {}", jarFile.getName(), e);
                                }
                                iterator.remove();
                            }
                        }
                    }
                }

                if (urlCache != null) {
                    Iterator<? extends Map.Entry<?, ?>> urlIterator =
                            urlCache.entrySet().iterator();
                    while (urlIterator.hasNext()) {
                        Map.Entry<?, ?> entry = urlIterator.next();
                        Object key = entry.getKey();
                        if (key instanceof JarFile) {
                            JarFile jarFile = (JarFile) key;
                            if (targetJarPaths.contains(jarFile.getName())) {
                                urlIterator.remove();
                            }
                        }
                    }
                }
            }
            log.info("Finished targeted deep clean of JarFileFactory global cache");
        } catch (ClassNotFoundException e) {
            // Configuration/Initialization errors -> Elevated to WARN
            log.warn("Deep clean failed: JarFileFactory class not found (non-HotSpot JVM?).", e);
        } catch (Exception e) {
            // Configuration/Initialization errors -> Elevated to WARN
            log.warn(
                    "Deep clean failed due to reflection restrictions. Please add '--add-opens java.base/sun.net.www.protocol.jar=ALL-UNNAMED' to JVM options.",
                    e);
        }
    }
}
