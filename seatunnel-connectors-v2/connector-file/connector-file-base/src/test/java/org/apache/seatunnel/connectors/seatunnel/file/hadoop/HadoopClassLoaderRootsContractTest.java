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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CodecPool;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * Pins the Hadoop internals that the Zeta engine relies on to release a job class loader
 * (apache/seatunnel#12456). {@code
 * org.apache.seatunnel.engine.core.classloader.ReleasedClassLoaderReferenceCleaner} detaches the
 * released loader from {@code WritableComparator.comparators} (through {@code getConf}/{@code
 * setConf} and {@code Configuration.getClassLoader()}) and removes the released loader's codec
 * classes from the four static registries of {@code CodecPool}, all reflectively because the engine
 * has no Hadoop dependency. This module has the shipped Hadoop uber jar on its class path. A Hadoop
 * bump that changes any of these shapes turns the engine step into a logged no-op and brings the
 * Metaspace leak back; this test turns red instead.
 */
public class HadoopClassLoaderRootsContractTest {

    private static final String[] CODEC_POOL_FIELDS = {
        "compressorPool", "decompressorPool", "compressorCounts", "decompressorCounts"
    };

    @Test
    void writableComparatorRegistryKeepsTheShapeTheEngineReliesOn() throws Exception {
        Field registryField = WritableComparator.class.getDeclaredField("comparators");
        registryField.setAccessible(true);
        Assertions.assertTrue(Modifier.isStatic(registryField.getModifiers()));
        Assertions.assertTrue(registryField.get(null) instanceof Map);

        Method getConf = WritableComparator.class.getMethod("getConf");
        Assertions.assertSame(Configuration.class, getConf.getReturnType());
        Assertions.assertNotNull(
                WritableComparator.class.getMethod("setConf", Configuration.class));
        Assertions.assertSame(
                ClassLoader.class, Configuration.class.getMethod("getClassLoader").getReturnType());
    }

    @Test
    void codecPoolRegistriesKeepTheShapeTheEngineReliesOn() throws Exception {
        for (String fieldName : CODEC_POOL_FIELDS) {
            Field field = CodecPool.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            Assertions.assertTrue(Modifier.isStatic(field.getModifiers()), fieldName);
            Object registry = field.get(null);
            Assertions.assertNotNull(registry, fieldName);
            // The idle pools are maps, the lease counters are Guava caches reached via asMap().
            // The cache implementation class is not public, hence setAccessible, as in the engine.
            Object view = registry;
            if (!(registry instanceof Map)) {
                Method asMap = registry.getClass().getMethod("asMap");
                asMap.setAccessible(true);
                view = asMap.invoke(registry);
            }
            Assertions.assertTrue(view instanceof Map, fieldName);
        }
    }
}
