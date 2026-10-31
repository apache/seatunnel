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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * loaders are left untouched. Cleanup is best-effort: every step catches {@link Throwable}, logs
 * the failure and never prevents the class loader from being released, because the loader is
 * already gone from the service when the cleaner runs and the caller still has other loaders of the
 * same task group to release.
 *
 * <p>The cleaner discovers the library classes by name through the released loader and never needs
 * the libraries on the engine class path. When a library is absent, the corresponding step is a
 * no-op.
 *
 * <p>Maintenance contract: the class, field and method names below are internals of third-party
 * libraries and were verified against mongodb-driver-core 4.7.1 ({@code connector-mongodb} pins
 * {@code mongo.driver.version}) and the Hadoop 3.1.4 and 3.3.6 uber jars shipped by SeaTunnel. A
 * layout change in a newer library turns the corresponding step into a logged no-op and brings the
 * leak back, so the shapes are asserted against the real jars by contract tests in {@code
 * connector-mongodb} ({@code MongoBufferPoolPrunerContractTest}) and {@code connector-file-base}
 * ({@code HadoopClassLoaderRootsContractTest}). Keeping library knowledge in the engine is a
 * conscious shortcut: the intended longer-term home for this logic is a connector-owned release
 * hook discovered through an SPI on the released loader, which needs a new public API.
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

    /**
     * {@code ClassLoader.findLoadedClass(String)}, which answers whether a loader already loaded a
     * class without loading it. It is protected, so it is reached by reflection; on JDK 16+ this
     * needs {@code --add-opens java.base/java.lang=ALL-UNNAMED}, which the shipped {@code
     * jvm_options} already contain. Null when it is not accessible.
     */
    private static final Method FIND_LOADED_CLASS = resolveFindLoadedClass();

    private final String mongoBufferPoolClass;
    private final String writableComparatorClass;
    private final String codecPoolClass;

    /**
     * Signatures (step, exception class, message) of failures that were already logged at WARN. A
     * layout mismatch with a user-supplied library fails identically on every release, which would
     * flood the worker log with the same stack trace, so later occurrences go to DEBUG.
     */
    private final Set<String> reportedFailures = ConcurrentHashMap.newKeySet();

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
     * count reached zero in non-cache mode. Never throws: each root is cleaned independently and a
     * failure in one of them, including an {@link Error} such as a Metaspace {@code
     * OutOfMemoryError} raised while touching classes of the released loader, does not stop the
     * remaining roots from being cleaned.
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
            // Only act on a pool class that the released loader already loaded. Resolving it here
            // would define it into the loader that is being discarded (com.mongodb is child-first)
            // and reading DEFAULT would run the static initializer, which starts the very pruner
            // thread this step exists to stop. A job that merely had the driver on its jar path
            // (for example a deployment that failed before any connector code ran) has no pruner.
            Class<?> poolType = findLoadedClass(released, mongoBufferPoolClass);
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
        } catch (Throwable e) {
            logStepFailure("stop the MongoDB buffer pool pruner", e);
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
            // Hadoop backs the registry with a ConcurrentHashMap, whose iterator tolerates
            // concurrent define() calls. Another Hadoop build could use a plain map, which would
            // throw ConcurrentModificationException against a running job, so such a registry is
            // iterated under its own monitor; for a concurrent map a private monitor is used,
            // which locks nothing shared.
            Object monitor = registry instanceof ConcurrentMap ? new Object() : registry;
            synchronized (monitor) {
                Iterator<? extends Map.Entry<?, ?>> entries =
                        ((Map<?, ?>) registry).entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<?, ?> entry = entries.next();
                    Object comparator = entry.getValue();
                    // A registration made by a class of the released loader can never be looked
                    // up by another job, and would pin the loader through its key or value class.
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
                    // The comparator is a shared singleton, so only reset the Configuration that
                    // was created for the released job. A Configuration of a running job is kept,
                    // and an unset Configuration is the state of a comparator that was never used.
                    // WritableComparator.get stores a caller's Configuration without any lock, so
                    // the value is re-read right before it is cleared: a running job that stored
                    // its own Configuration in between keeps it. This narrows the window, it
                    // cannot close it from outside Hadoop.
                    Object conf = getConf.invoke(comparator);
                    if (conf != null
                            && getConfClassLoader.invoke(conf) == released
                            && getConf.invoke(comparator) == conf) {
                        setConf.invoke(comparator, new Object[] {null});
                        detached++;
                    }
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
        } catch (Throwable e) {
            logStepFailure("clean the Hadoop WritableComparator registry", e);
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
            } catch (Throwable e) {
                logStepFailure("clean Hadoop CodecPool." + fieldName, e);
            }
        }
        if (removed > 0) {
            log.info(
                    "Removed {} Hadoop CodecPool entries keyed by codec classes of released classloader {}",
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
     * Logs a failed cleanup step. The first failure with a given signature is logged at WARN with
     * its stack trace, identical later failures only at DEBUG, because a library layout that does
     * not match fails the same way on every single release and the loader is released regardless.
     */
    private void logStepFailure(String step, Throwable failure) {
        String signature = step + ": " + failure.getClass().getName() + ": " + failure.getMessage();
        if (reportedFailures.add(signature)) {
            log.warn(
                    "Failed to {} for a released classloader, identical failures are logged at DEBUG from now on",
                    step,
                    failure);
        } else {
            log.debug("Failed to {} for a released classloader", step, failure);
        }
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

    /**
     * Returns the class only if the released loader already loaded it (as defining or initiating
     * loader), null otherwise. Unlike {@link #findVisibleClass} this never defines a new class into
     * the loader. When {@code findLoadedClass} is not accessible the lookup falls back to resolving
     * the class, which keeps the leak fixed at the price of the extra work described in {@link
     * #stopMongoBufferPoolPruner}.
     */
    private static Class<?> findLoadedClass(ClassLoader loader, String className)
            throws ReflectiveOperationException {
        if (FIND_LOADED_CLASS == null) {
            return findVisibleClass(loader, className);
        }
        return (Class<?>) FIND_LOADED_CLASS.invoke(loader, className);
    }

    private static Method resolveFindLoadedClass() {
        try {
            Method findLoadedClass =
                    ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
            findLoadedClass.setAccessible(true);
            return findLoadedClass;
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn(
                    "ClassLoader.findLoadedClass is not accessible, released classloaders will resolve "
                            + "the MongoDB buffer pool class instead of checking whether the job loaded it. "
                            + "Add --add-opens java.base/java.lang=ALL-UNNAMED to the JVM options to avoid this.",
                    e);
            return null;
        }
    }

    private static boolean isDefinedBy(Object classKey, ClassLoader loader) {
        return classKey instanceof Class && ((Class<?>) classKey).getClassLoader() == loader;
    }
}
