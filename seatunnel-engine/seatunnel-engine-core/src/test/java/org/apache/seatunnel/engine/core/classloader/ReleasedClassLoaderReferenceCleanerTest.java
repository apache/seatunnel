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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

/**
 * Covers the references that MongoDB and Hadoop keep to a job class loader after the engine
 * released it (apache/seatunnel#12456). The fixtures mirror the exact shapes of the real libraries
 * that were found in the heap dump: a static {@code DEFAULT} pool with a package-private {@code
 * disablePruning()}, and a static {@code comparators} registry of shared singletons whose {@code
 * Configuration} captures the job class loader. A regression here means a released job class
 * loader, and the Metaspace of all its classes, is pinned for the lifetime of the worker again.
 */
public class ReleasedClassLoaderReferenceCleanerTest {

    private static final String POOL = FixtureBufferPool.class.getName();
    private static final String COMPARATOR = FixtureWritableComparator.class.getName();

    private final ReleasedClassLoaderReferenceCleaner cleaner =
            new ReleasedClassLoaderReferenceCleaner(POOL, COMPARATOR);

    @AfterEach
    void clearComparatorRegistry() {
        FixtureWritableComparator.clear();
    }

    /**
     * Guards the BufferPoolPruner root: the pool of a released job loader must be shut down, or its
     * thread keeps the loader alive through the thread factory class and the inherited access
     * control context.
     */
    @Test
    void stopsPrunerOfPoolDefinedByReleasedLoader() throws Exception {
        SeaTunnelChildFirstClassLoader released = newLoaderOwning("java.");
        Class<?> poolType = Class.forName(POOL, true, released);
        Assertions.assertSame(released, poolType.getClassLoader());
        BooleanSupplier pool = (BooleanSupplier) poolType.getField("DEFAULT").get(null);
        Assertions.assertTrue(pool.getAsBoolean());

        cleaner.clean(released);

        Assertions.assertFalse(pool.getAsBoolean());
    }

    /**
     * Guards other jobs: a pool defined by an ancestor loader is shared process-wide, so releasing
     * one job must not stop the pruner that the remaining jobs still rely on.
     */
    @Test
    void keepsPrunerOfPoolDefinedByAncestorLoader() {
        ClassLoader released = new SeaTunnelChildFirstClassLoader(Collections.emptyList());

        cleaner.clean(released);

        Assertions.assertTrue(FixtureBufferPool.DEFAULT.getAsBoolean());
    }

    /**
     * Guards the WritableComparator.comparators root: the shared singleton of a released job must
     * stop holding that job's Configuration, while the registration and the Configuration of a job
     * that is still running stay untouched.
     */
    @Test
    void detachesOnlyConfigurationOfReleasedLoader() {
        ClassLoader released = new SeaTunnelChildFirstClassLoader(Collections.emptyList());
        ClassLoader running = new SeaTunnelChildFirstClassLoader(Collections.emptyList());
        FixtureWritableComparator releasedJobComparator =
                FixtureWritableComparator.define(String.class);
        FixtureWritableComparator runningJobComparator =
                FixtureWritableComparator.define(Integer.class);
        FixtureConfiguration runningConf = new FixtureConfiguration(running);
        FixtureWritableComparator.get(String.class, new FixtureConfiguration(released));
        FixtureWritableComparator.get(Integer.class, runningConf);

        cleaner.clean(released);

        Assertions.assertNull(releasedJobComparator.getConf());
        Assertions.assertSame(runningConf, runningJobComparator.getConf());
        Assertions.assertTrue(FixtureWritableComparator.isRegistered(String.class));
        Assertions.assertTrue(FixtureWritableComparator.isRegistered(Integer.class));
    }

    /**
     * Guards registrations made by classes of the released loader: they can never be looked up by
     * another job and would pin the loader through the key class, so they are removed, while
     * registrations of unrelated classes stay.
     */
    @Test
    void removesRegistrationsKeyedByClassOfReleasedLoader() throws Exception {
        SeaTunnelChildFirstClassLoader released = newLoaderOwning("java.", COMPARATOR);
        Class<?> ownedKey = Class.forName(FixtureKey.class.getName(), false, released);
        Assertions.assertSame(released, ownedKey.getClassLoader());
        FixtureWritableComparator.define(ownedKey);
        FixtureWritableComparator.define(String.class);

        cleaner.clean(released);

        Assertions.assertFalse(FixtureWritableComparator.isRegistered(ownedKey));
        Assertions.assertTrue(FixtureWritableComparator.isRegistered(String.class));
    }

    /**
     * Guards the wiring in the service: the cleanup must run when the last reference of a loader is
     * released and not earlier, because a loader that is still referenced is used by a job.
     */
    @Test
    void serviceCleansOnlyWhenLastReferenceIsReleased() {
        DefaultClassLoaderService service = new DefaultClassLoaderService(false, null, cleaner);
        ClassLoader loader = service.getClassLoader(7L, Collections.emptyList());
        service.getClassLoader(7L, Collections.emptyList());
        FixtureWritableComparator comparator = FixtureWritableComparator.define(String.class);
        FixtureWritableComparator.get(String.class, new FixtureConfiguration(loader));

        service.releaseClassLoader(7L, Collections.emptyList());
        Assertions.assertNotNull(comparator.getConf());

        service.releaseClassLoader(7L, Collections.emptyList());
        Assertions.assertNull(comparator.getConf());
    }

    /**
     * Guards cache mode: the cached loader is shared by later jobs with the same jars and is never
     * physically released, so its references must never be cleaned.
     */
    @Test
    void serviceKeepsReferencesOfSharedLoaderInCacheMode() {
        DefaultClassLoaderService service = new DefaultClassLoaderService(true, null, cleaner);
        ClassLoader loader = service.getClassLoader(8L, Collections.emptyList());
        FixtureWritableComparator comparator = FixtureWritableComparator.define(String.class);
        FixtureWritableComparator.get(String.class, new FixtureConfiguration(loader));

        service.releaseClassLoader(8L, Collections.emptyList());

        Assertions.assertNotNull(comparator.getConf());
    }

    /**
     * Creates a child-first loader that defines its own copy of the fixtures, which is what the
     * connector jars do for the real libraries. Classes whose name starts with one of the given
     * prefixes still come from the parent.
     */
    private static SeaTunnelChildFirstClassLoader newLoaderOwning(String... parentFirstPrefixes) {
        URL testClasses =
                FixtureBufferPool.class.getProtectionDomain().getCodeSource().getLocation();
        return new SeaTunnelChildFirstClassLoader(
                Collections.singletonList(testClasses), parentFirstPrefixes);
    }

    /** Mirrors com.mongodb.internal.connection.PowerOfTwoBufferPool. */
    public static final class FixtureBufferPool implements BooleanSupplier {
        public static final FixtureBufferPool DEFAULT = new FixtureBufferPool();

        private volatile boolean pruning = true;

        private FixtureBufferPool() {}

        void disablePruning() {
            pruning = false;
        }

        @Override
        public boolean getAsBoolean() {
            return pruning;
        }
    }

    /** Mirrors org.apache.hadoop.io.WritableComparator. */
    public static class FixtureWritableComparator {
        private static final ConcurrentHashMap<Class<?>, FixtureWritableComparator> comparators =
                new ConcurrentHashMap<>();

        private FixtureConfiguration conf;

        public static FixtureWritableComparator define(Class<?> keyClass) {
            FixtureWritableComparator comparator = new FixtureWritableComparator();
            comparators.put(keyClass, comparator);
            return comparator;
        }

        public static FixtureWritableComparator get(
                Class<?> keyClass, FixtureConfiguration configuration) {
            FixtureWritableComparator comparator = comparators.get(keyClass);
            comparator.setConf(configuration);
            return comparator;
        }

        static boolean isRegistered(Class<?> keyClass) {
            return comparators.containsKey(keyClass);
        }

        static void clear() {
            comparators.clear();
        }

        public FixtureConfiguration getConf() {
            return conf;
        }

        public void setConf(FixtureConfiguration conf) {
            this.conf = conf;
        }
    }

    /** Mirrors org.apache.hadoop.conf.Configuration, which captures the context class loader. */
    public static class FixtureConfiguration {
        private final ClassLoader classLoader;

        public FixtureConfiguration(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }

    /** A key class that a loader can define on its own, like a connector-specific Writable. */
    public static class FixtureKey {}
}
