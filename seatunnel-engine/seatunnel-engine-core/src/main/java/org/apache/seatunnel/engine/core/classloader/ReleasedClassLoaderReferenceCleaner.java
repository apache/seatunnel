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

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

/**
 * Breaks the strong references that third-party libraries keep to a job class loader after {@link
 * DefaultClassLoaderService} has dropped the last reference to it.
 *
 * <p>With {@code classloader-cache-mode=false} every job gets its own {@code
 * SeaTunnelChildFirstClassLoader} per jar set, and the engine forgets it once the reference count
 * reaches zero. A forgotten loader is only collectable when nothing else points to it, but three
 * library-level roots were observed (apache/seatunnel#12456) to keep it, and the Metaspace it
 * defines, alive for the whole life of the worker:
 *
 * <ul>
 *   <li>MongoDB driver: {@code PowerOfTwoBufferPool.DEFAULT} starts a never-ending {@code
 *       BufferPoolPruner} thread from a static initializer. The thread pins the loader through its
 *       thread factory class and through its inherited access control context.
 *   <li>Hadoop: {@code WritableComparator.comparators} is a static registry of shared comparator
 *       singletons, and {@code WritableComparator.get(Class, Configuration)} stores the caller's
 *       {@code Configuration} into the shared singleton. A {@code Configuration} captures the
 *       thread context class loader at construction time, which is the job loader. Hadoop classes
 *       are always loaded parent-first (see {@code SeaTunnelChildFirstClassLoader}), so the
 *       registry lives in the application class loader and outlives every job.
 *   <li>Hadoop: {@code CodecPool} keeps idle compressors and lease counters in static maps keyed by
 *       the codec's {@code Class}. When the codec classes are bundled in a connector jar (for
 *       example the shaded Parquet {@code SnappyCompressor} of the Hive connector), every job
 *       loader defines a new key class, so each finished job leaves one more entry, and one more
 *       pinned loader, behind. This is the only one of the three roots that grows per job.
 * </ul>
 *
 * <p>Only references that are provably owned by the released loader are touched. Process-wide
 * singletons that are defined by an ancestor class loader may still be used by other jobs, so they
 * are never stopped or cleared, and registry entries or configurations that belong to other class
 * loaders are left untouched. Cleanup is best-effort: a failure is logged and never prevents the
 * class loader from being released.
 *
 * <p>The cleaner discovers the library classes by name through the released loader and never needs
 * the libraries on the engine class path. When a library is absent, the corresponding step is a
 * no-op.
 */
@Slf4j
final class ReleasedClassLoaderReferenceCleaner {

    static final String MONGO_BUFFER_POOL_CLASS =
            "com.mongodb.internal.connection.PowerOfTwoBufferPool";

    static final String HADOOP_WRITABLE_COMPARATOR_CLASS =
            "org.apache.hadoop.io.WritableComparator";

    static final String HADOOP_CODEC_POOL_CLASS = "org.apache.hadoop.io.compress.CodecPool";

    /**
     * Static fields of {@code CodecPool} that are keyed by the codec class: the idle compressor and
     * decompressor pools (plain maps guarded by their own monitor) and the lease counters (Guava
     * caches). The layout is identical in the Hadoop 3.1.4 and 3.3.6 uber jars shipped by
     * SeaTunnel.
     */
    private static final String[] CODEC_POOL_FIELDS = {
        "compressorPool", "decompressorPool", "compressorCounts", "decompressorCounts"
    };

    private final String mongoBufferPoolClass;
    private final String writableComparatorClass;
    private final String codecPoolClass;

    ReleasedClassLoaderReferenceCleaner() {
        this(MONGO_BUFFER_POOL_CLASS, HADOOP_WRITABLE_COMPARATOR_CLASS, HADOOP_CODEC_POOL_CLASS);
    }

    /** Allows tests to point the cleaner at fixture classes instead of the real libraries. */
    ReleasedClassLoaderReferenceCleaner(
            String mongoBufferPoolClass, String writableComparatorClass, String codecPoolClass) {
        this.mongoBufferPoolClass = mongoBufferPoolClass;
        this.writableComparatorClass = writableComparatorClass;
        this.codecPoolClass = codecPoolClass;
    }

    /**
     * Cleans the library references owned by a class loader that the engine has just released. Must
     * only be called once no job uses the loader any more, which is the case when its reference
     * count reached zero in non-cache mode.
     *
     * @param released the class loader that was just removed from the class loader service
     */
    void clean(ClassLoader released) {
        if (released == null) {
            return;
        }
        stopMongoBufferPoolPruner(released);
        detachHadoopWritableComparators(released);
        removeHadoopCodecPoolEntries(released);
    }

    private void stopMongoBufferPoolPruner(ClassLoader released) {
        try {
            Class<?> poolType = findVisibleClass(released, mongoBufferPoolClass);
            // A pool defined by an ancestor loader is shared by every job, keep it running. Only a
            // pool defined by the released loader is exclusively owned by this job.
            if (poolType == null || poolType.getClassLoader() != released) {
                return;
            }
            Field defaultPool = poolType.getDeclaredField("DEFAULT");
            defaultPool.setAccessible(true);
            Object pool = defaultPool.get(null);
            // disablePruning() is the driver's own shutdown of the pruner executor. The pool stays
            // usable, it just stops scheduling the pruning task and lets the thread exit.
            Method disablePruning = poolType.getDeclaredMethod("disablePruning");
            disablePruning.setAccessible(true);
            disablePruning.invoke(pool);
            log.info("Stopped MongoDB buffer pool pruner of released classloader {}", released);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            log.warn("Failed to stop MongoDB buffer pool pruner of released classloader", e);
        }
    }

    private void detachHadoopWritableComparators(ClassLoader released) {
        try {
            Class<?> comparatorType = findVisibleClass(released, writableComparatorClass);
            // Statics of a class defined by the released loader are collected together with it.
            if (comparatorType == null || comparatorType.getClassLoader() == released) {
                return;
            }
            Field registryField = comparatorType.getDeclaredField("comparators");
            registryField.setAccessible(true);
            Object registry = registryField.get(null);
            if (!(registry instanceof Map)) {
                log.warn(
                        "Skip Hadoop WritableComparator cleanup, unexpected registry type {}",
                        registry == null ? null : registry.getClass().getName());
                return;
            }
            Method getConf = comparatorType.getMethod("getConf");
            Class<?> confType = getConf.getReturnType();
            Method setConf = comparatorType.getMethod("setConf", confType);
            Method getConfClassLoader = confType.getMethod("getClassLoader");

            int detached = 0;
            int removed = 0;
            Iterator<? extends Map.Entry<?, ?>> entries =
                    ((Map<?, ?>) registry).entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<?, ?> entry = entries.next();
                Object comparator = entry.getValue();
                // A registration made by a class of the released loader can never be looked up by
                // another job, and would pin the loader through its key or value class.
                if (isDefinedBy(entry.getKey(), released)
                        || (comparator != null
                                && comparator.getClass().getClassLoader() == released)) {
                    entries.remove();
                    removed++;
                    continue;
                }
                if (comparator == null) {
                    continue;
                }
                // The comparator is a shared singleton, so only reset the Configuration that was
                // created for the released job. A Configuration of a running job is kept, and an
                // unset Configuration is the state of a comparator that was never used.
                Object conf = getConf.invoke(comparator);
                if (conf != null && getConfClassLoader.invoke(conf) == released) {
                    setConf.invoke(comparator, new Object[] {null});
                    detached++;
                }
            }
            if (detached > 0 || removed > 0) {
                log.info(
                        "Detached {} Hadoop WritableComparator configuration(s) and removed {} registration(s) "
                                + "of released classloader {}",
                        detached,
                        removed,
                        released);
            }
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            log.warn(
                    "Failed to clean Hadoop WritableComparator registry for released classloader",
                    e);
        }
    }

    private void removeHadoopCodecPoolEntries(ClassLoader released) {
        Class<?> poolType = findVisibleClass(released, codecPoolClass);
        // Statics of a class defined by the released loader are collected together with it.
        if (poolType == null || poolType.getClassLoader() == released) {
            return;
        }
        int removed = 0;
        for (String fieldName : CODEC_POOL_FIELDS) {
            // Each registry is handled on its own, so a field that is missing in another Hadoop
            // layout does not stop the others from being cleaned.
            try {
                removed += removeEntriesKeyedByClassOf(poolType, fieldName, released);
            } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
                log.warn(
                        "Failed to clean Hadoop CodecPool.{} for released classloader",
                        fieldName,
                        e);
            }
        }
        if (removed > 0) {
            log.info(
                    "Removed {} Hadoop CodecPool entrie(s) keyed by codec classes of released classloader {}",
                    removed,
                    released);
        }
    }

    /**
     * Removes the entries of a static {@code Class}-keyed registry whose key class is defined by
     * the released loader. Such an entry can never be looked up by another job, because a different
     * loader defines a different {@code Class} object, so removing it only drops idle state of the
     * released job. The idle compressor instances in the removed values are not ended explicitly,
     * their native resources are released by their own finalizers or direct buffer cleaners.
     */
    private static int removeEntriesKeyedByClassOf(
            Class<?> ownerType, String fieldName, ClassLoader released)
            throws ReflectiveOperationException {
        Field field = ownerType.getDeclaredField(fieldName);
        field.setAccessible(true);
        Object registry = field.get(null);
        if (registry == null) {
            return 0;
        }
        int removed = 0;
        // CodecPool.borrow and payback synchronize on the idle pool map itself, use the same
        // monitor so a concurrent job never observes a half-removed entry.
        synchronized (registry) {
            Map<?, ?> map = asMap(registry);
            if (map == null) {
                log.warn(
                        "Skip Hadoop CodecPool.{} cleanup, unexpected registry type {}",
                        fieldName,
                        registry.getClass().getName());
                return 0;
            }
            Iterator<? extends Map.Entry<?, ?>> entries = map.entrySet().iterator();
            while (entries.hasNext()) {
                if (isDefinedBy(entries.next().getKey(), released)) {
                    entries.remove();
                    removed++;
                }
            }
        }
        return removed;
    }

    /**
     * Returns a mutable map view of a registry: the map itself, or the {@code asMap()} view of a
     * Guava cache. The cache implementation class is not public, hence {@code setAccessible}.
     */
    private static Map<?, ?> asMap(Object registry) throws ReflectiveOperationException {
        if (registry instanceof Map) {
            return (Map<?, ?>) registry;
        }
        Method asMap = registry.getClass().getMethod("asMap");
        asMap.setAccessible(true);
        Object view = asMap.invoke(registry);
        return view instanceof Map ? (Map<?, ?>) view : null;
    }

    /**
     * Resolves a class through the released loader without initializing it. Delegation makes this
     * return the exact definition the job used: the loader's own copy for child-first classes, or
     * the ancestor's copy for parent-first ones such as Hadoop.
     */
    private static Class<?> findVisibleClass(ClassLoader loader, String className) {
        try {
            return Class.forName(className, false, loader);
        } catch (ClassNotFoundException | LinkageError e) {
            log.debug("Class {} is not visible from released classloader {}", className, loader);
            return null;
        }
    }

    private static boolean isDefinedBy(Object classKey, ClassLoader loader) {
        return classKey instanceof Class && ((Class<?>) classKey).getClassLoader() == loader;
    }
}
