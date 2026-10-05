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
 * reaches zero. A forgotten loader is only collectable when nothing else points to it, but two
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

    private final String mongoBufferPoolClass;
    private final String writableComparatorClass;

    ReleasedClassLoaderReferenceCleaner() {
        this(MONGO_BUFFER_POOL_CLASS, HADOOP_WRITABLE_COMPARATOR_CLASS);
    }

    /** Allows tests to point the cleaner at fixture classes instead of the real libraries. */
    ReleasedClassLoaderReferenceCleaner(
            String mongoBufferPoolClass, String writableComparatorClass) {
        this.mongoBufferPoolClass = mongoBufferPoolClass;
        this.writableComparatorClass = writableComparatorClass;
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
