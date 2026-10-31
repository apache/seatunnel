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

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final String CODEC_POOL = FixtureCodecPool.class.getName();
    private static final String FAILING_POOL = FixtureFailingBufferPool.class.getName();
    private static final String WITNESS = FixtureInitializationWitness.class.getName();

    private final ReleasedClassLoaderReferenceCleaner cleaner =
            new ReleasedClassLoaderReferenceCleaner(POOL, COMPARATOR, CODEC_POOL);

    @AfterEach
    void clearFixtureRegistries() {
        FixtureWritableComparator.clear();
        FixtureCodecPool.clear();
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
     * one job must not stop the pruner that the remaining jobs still rely on. The job loads the
     * pool class first, so it is the ownership check and not the loaded-class gate that keeps it.
     */
    @Test
    void keepsPrunerOfPoolDefinedByAncestorLoader() throws Exception {
        ClassLoader released = new SeaTunnelChildFirstClassLoader(Collections.emptyList());
        Class<?> poolType = Class.forName(POOL, true, released);
        Assertions.assertNotSame(released, poolType.getClassLoader());

        cleaner.clean(released);

        Assertions.assertTrue(FixtureBufferPool.DEFAULT.getAsBoolean());
    }

    /**
     * Guards against the cleaner creating what it is meant to remove: a job that never loaded the
     * pool class has no pruner, so the released loader must neither get the class defined into it
     * nor have its static initializer (which starts the pruner thread in the real driver) run.
     */
    @Test
    void leavesPoolClassUnloadedWhenReleasedLoaderNeverUsedIt() throws Exception {
        SeaTunnelChildFirstClassLoader released = newLoaderOwning("java.", WITNESS);
        int initializationsBefore = FixtureInitializationWitness.POOL_INITIALIZATIONS.get();

        cleaner.clean(released);

        Assertions.assertEquals(
                initializationsBefore, FixtureInitializationWitness.POOL_INITIALIZATIONS.get());
        Assertions.assertNull(findLoadedClass(released, POOL));
    }

    /**
     * Guards the best-effort contract: an Error raised by one root (here the pool's static
     * initializer, like a Metaspace OutOfMemoryError on a starved worker) must not escape clean()
     * and must not stop the Hadoop roots of the same loader from being cleaned.
     */
    @Test
    void cleansRemainingRootsWhenOneRootThrowsAnError() throws Exception {
        ReleasedClassLoaderReferenceCleaner failingPoolCleaner =
                new ReleasedClassLoaderReferenceCleaner(FAILING_POOL, COMPARATOR, CODEC_POOL);
        SeaTunnelChildFirstClassLoader released = newLoaderOwning("java.", COMPARATOR, CODEC_POOL);
        // The job loaded the pool class but its static initializer has not run yet.
        Class.forName(FAILING_POOL, false, released);
        Class<?> ownedKey = Class.forName(FixtureKey.class.getName(), false, released);
        FixtureWritableComparator.define(ownedKey);
        FixtureCodecPool.lease(ownedKey);

        failingPoolCleaner.clean(released);

        Assertions.assertFalse(FixtureWritableComparator.isRegistered(ownedKey));
        Assertions.assertFalse(FixtureCodecPool.isTrackedInAny(ownedKey));
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
     * Guards the CodecPool root, the one that grows per job: idle pools and lease counters keyed by
     * a codec class of the released loader must be dropped from all four registries, while the
     * entries of an ancestor-defined codec and of a loader that is still running stay.
     */
    @Test
    void removesCodecPoolEntriesKeyedByClassOfReleasedLoaderOnly() throws Exception {
        SeaTunnelChildFirstClassLoader released = newLoaderOwning("java.", COMPARATOR, CODEC_POOL);
        SeaTunnelChildFirstClassLoader running = newLoaderOwning("java.", COMPARATOR, CODEC_POOL);
        Class<?> releasedCodec = Class.forName(FixtureKey.class.getName(), false, released);
        Class<?> runningCodec = Class.forName(FixtureKey.class.getName(), false, running);
        Assertions.assertNotSame(releasedCodec, runningCodec);
        FixtureCodecPool.lease(releasedCodec);
        FixtureCodecPool.lease(runningCodec);
        FixtureCodecPool.lease(String.class);

        cleaner.clean(released);

        Assertions.assertFalse(FixtureCodecPool.isTrackedInAny(releasedCodec));
        Assertions.assertTrue(FixtureCodecPool.isTrackedInAll(runningCodec));
        Assertions.assertTrue(FixtureCodecPool.isTrackedInAll(String.class));
    }

    /**
     * Guards the sharing contract of a CodecPool defined by an ancestor loader: it is cleaned by
     * key ownership only and is never replaced or cleared, so another job keeps its idle pool.
     */
    @Test
    void keepsCodecPoolEntriesWhenNoKeyBelongsToReleasedLoader() {
        ClassLoader released = new SeaTunnelChildFirstClassLoader(Collections.emptyList());
        FixtureCodecPool.lease(String.class);
        FixtureCodecPool.lease(Integer.class);

        cleaner.clean(released);

        Assertions.assertTrue(FixtureCodecPool.isTrackedInAll(String.class));
        Assertions.assertTrue(FixtureCodecPool.isTrackedInAll(Integer.class));
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
     * Guards the operator kill switch: with the cleaner disabled the service must still release
     * loaders normally and leave the library references alone.
     */
    @Test
    void serviceReleasesWithoutCleaningWhenCleanerIsDisabled() {
        DefaultClassLoaderService service = new DefaultClassLoaderService(false, null, null);
        ClassLoader loader = service.getClassLoader(9L, Collections.emptyList());
        FixtureWritableComparator comparator = FixtureWritableComparator.define(String.class);
        FixtureWritableComparator.get(String.class, new FixtureConfiguration(loader));

        service.releaseClassLoader(9L, Collections.emptyList());

        Assertions.assertNotNull(comparator.getConf());
        Assertions.assertFalse(
                service.queryClassLoaderById(9L, Collections.emptyList()).isPresent());
    }

    /** Asks the loader itself whether it already loaded a class, without loading it. */
    private static Class<?> findLoadedClass(ClassLoader loader, String className) throws Exception {
        Method findLoadedClass =
                ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
        findLoadedClass.setAccessible(true);
        return (Class<?>) findLoadedClass.invoke(loader, className);
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

    /**
     * Mirrors com.mongodb.internal.connection.PowerOfTwoBufferPool, whose static initializer starts
     * the pruner thread. Every initialization is counted in the witness so a test can prove that
     * the initializer did not run.
     */
    public static final class FixtureBufferPool implements BooleanSupplier {
        public static final FixtureBufferPool DEFAULT = new FixtureBufferPool();

        static {
            FixtureInitializationWitness.POOL_INITIALIZATIONS.incrementAndGet();
        }

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

    /**
     * Counts initializations of {@link FixtureBufferPool}. Loaded parent-first by the tests that
     * need it, so the copy of the pool class defined by a job loader reports to the shared counter.
     */
    public static final class FixtureInitializationWitness {
        public static final AtomicInteger POOL_INITIALIZATIONS = new AtomicInteger();
    }

    /**
     * A pool whose static initializer fails with an Error, which is what a Metaspace
     * OutOfMemoryError in the real pool's initializer looks like to the cleaner: the Error leaves
     * {@code Field.get} unwrapped.
     */
    public static final class FixtureFailingBufferPool {
        public static final Object DEFAULT = failInitialization();

        private static Object failInitialization() {
            throw new OutOfMemoryError("simulated Metaspace exhaustion in the pool initializer");
        }

        void disablePruning() {}
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

    /**
     * Mirrors org.apache.hadoop.io.compress.CodecPool: idle pools in plain maps and lease counters
     * in a cache whose implementation class is not public, all keyed by the codec class.
     */
    public static class FixtureCodecPool {
        private static final Map<Class<?>, Set<Object>> compressorPool = new HashMap<>();
        private static final Map<Class<?>, Set<Object>> decompressorPool = new HashMap<>();
        private static final FixtureCache compressorCounts = new FixtureCache();
        private static final FixtureCache decompressorCounts = new FixtureCache();

        static void lease(Class<?> codecType) {
            synchronized (compressorPool) {
                compressorPool.put(codecType, new HashSet<>());
            }
            synchronized (decompressorPool) {
                decompressorPool.put(codecType, new HashSet<>());
            }
            compressorCounts.counts.put(codecType, new AtomicInteger());
            decompressorCounts.counts.put(codecType, new AtomicInteger());
        }

        static boolean isTrackedInAll(Class<?> codecType) {
            return compressorPool.containsKey(codecType)
                    && decompressorPool.containsKey(codecType)
                    && compressorCounts.counts.containsKey(codecType)
                    && decompressorCounts.counts.containsKey(codecType);
        }

        static boolean isTrackedInAny(Class<?> codecType) {
            return compressorPool.containsKey(codecType)
                    || decompressorPool.containsKey(codecType)
                    || compressorCounts.counts.containsKey(codecType)
                    || decompressorCounts.counts.containsKey(codecType);
        }

        static void clear() {
            compressorPool.clear();
            decompressorPool.clear();
            compressorCounts.counts.clear();
            decompressorCounts.counts.clear();
        }
    }

    /** Mirrors the shaded Guava LoadingCache, which is only reachable through asMap(). */
    static final class FixtureCache {
        private final ConcurrentMap<Class<?>, AtomicInteger> counts = new ConcurrentHashMap<>();

        public ConcurrentMap<Class<?>, AtomicInteger> asMap() {
            return counts;
        }
    }

    /** A key class that a loader can define on its own, like a connector-specific Writable. */
    public static class FixtureKey {}
}
