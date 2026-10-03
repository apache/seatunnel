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
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DefaultClassLoaderService implements ClassLoaderService {
    private final boolean cacheMode;
    private final Map<Long, Map<String, ClassLoader>> classLoaderCache;
    private final Map<Long, Map<String, AtomicInteger>> classLoaderReferenceCount;
    private final NodeEngine nodeEngine;
    private final JarPathResolver jarPathResolver;
    public static final String SKIP_CHECK_JAR = "CLASSLOADER_SERVICE_SKIP_CHECK_JAR";

    /**
     * Creates a service using the original jar URLs without deployment-specific resolution.
     *
     * @param cacheMode whether loaders with equal jar identities are shared across jobs
     * @param nodeEngine owning node for local artifact checks, or null when no node is available
     */
    public DefaultClassLoaderService(boolean cacheMode, NodeEngine nodeEngine) {
        this(cacheMode, nodeEngine, JarPathResolver.identity());
    }

    /**
     * Creates a service with an explicit instance-scoped jar resolver.
     *
     * <p>The service retains the resolver without closing it. Original URLs remain the cache and
     * release identities; resolved URLs are used only for local validation and loader construction.
     *
     * @param cacheMode whether loaders with equal jar identities are shared across jobs
     * @param nodeEngine owning node for local artifact checks, or null when no node is available
     * @param jarPathResolver stable resolver for this service's lifetime
     * @throws NullPointerException if the resolver is null
     */
    public DefaultClassLoaderService(
            boolean cacheMode, NodeEngine nodeEngine, JarPathResolver jarPathResolver) {
        this.cacheMode = cacheMode;
        this.nodeEngine = nodeEngine;
        this.jarPathResolver = Objects.requireNonNull(jarPathResolver, "jarPathResolver");
        classLoaderCache = new ConcurrentHashMap<>();
        classLoaderReferenceCount = new ConcurrentHashMap<>();
        log.info("start classloader service" + (cacheMode ? " with cache mode" : ""));
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
            Collection<URL> localJars = jarPathResolver.resolve(jars);
            if (Objects.nonNull(nodeEngine)
                    && !Boolean.parseBoolean(
                            System.getenv().getOrDefault(SKIP_CHECK_JAR, "false"))) {
                for (URL jar : localJars) {
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
            ClassLoader classLoader = new SeaTunnelChildFirstClassLoader(localJars);
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
                            log.info("recycle classloader for thread " + thread.getName());
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
        classLoaderCache.clear();
        classLoaderReferenceCount.clear();
    }
}
