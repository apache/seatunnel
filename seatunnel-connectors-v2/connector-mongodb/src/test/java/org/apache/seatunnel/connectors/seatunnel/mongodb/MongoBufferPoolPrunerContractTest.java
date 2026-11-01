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

package org.apache.seatunnel.connectors.seatunnel.mongodb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Pins the driver internals that the Zeta engine relies on to release a job class loader
 * (apache/seatunnel#12456). {@code
 * org.apache.seatunnel.engine.core.classloader.ReleasedClassLoaderReferenceCleaner} stops the
 * {@code BufferPoolPruner} thread of a released loader by reading {@code
 * PowerOfTwoBufferPool.DEFAULT} and invoking {@code disablePruning()} reflectively; the engine has
 * no driver dependency, so this module, which pins {@code mongo.driver.version}, is where the real
 * class is on the class path. A driver bump that renames or removes either member turns the engine
 * step into a logged no-op and brings the Metaspace leak back; this test turns red instead.
 */
public class MongoBufferPoolPrunerContractTest {

    private static final String POOL_CLASS = "com.mongodb.internal.connection.PowerOfTwoBufferPool";

    @Test
    void defaultPoolAndDisablePruningKeepTheShapeTheEngineReliesOn() throws Exception {
        // Resolved without initialization, exactly as the engine does: initializing the class
        // would start the pruner thread in this test JVM.
        Class<?> poolType = Class.forName(POOL_CLASS, false, getClass().getClassLoader());

        Field defaultPool = poolType.getDeclaredField("DEFAULT");
        Assertions.assertTrue(Modifier.isStatic(defaultPool.getModifiers()));
        Assertions.assertSame(poolType, defaultPool.getType());

        Method disablePruning = poolType.getDeclaredMethod("disablePruning");
        Assertions.assertFalse(Modifier.isStatic(disablePruning.getModifiers()));
        Assertions.assertEquals(0, disablePruning.getParameterCount());
    }
}
