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

import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DefaultClassLoaderService implements ClassLoaderService {
    private final boolean cacheMode;
    private final Map<Long, Map<String, ClassLoader>> classLoaderCache;
    private final Map<Long, Map<String, AtomicInteger>> classLoaderReferenceCount;

    public DefaultClassLoaderService(boolean cacheMode) {
        this.cacheMode = cacheMode;
        classLoaderCache = new ConcurrentHashMap<>();
        classLoaderReferenceCount = new ConcurrentHashMap<>();
        log.info("start classloader service" + (cacheMode ? " with cache mode" : ""));
    }

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
    Optional<ClassLoader> queryClassLoaderById(long jobId, Collection<URL> jars) {
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
    int queryClassLoaderReferenceCount(long jobId, Collection<URL> jars) {
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
    int queryClassLoaderCount() {
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
