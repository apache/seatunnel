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

package org.apache.seatunnel.benchmark;

import java.lang.reflect.Field;

/** Read access to engine internals that a benchmark fixture observes but has no getter for. */
final class BenchmarkReflection {

    private BenchmarkReflection() {}

    /**
     * Returns the named declared field, made accessible.
     *
     * @throws IllegalStateException naming the class and field when the field no longer exists, so
     *     an engine-internals rename fails the benchmark at setup instead of skewing its numbers
     */
    static Field requireField(Class<?> owner, String name) {
        try {
            Field field = owner.getDeclaredField(name);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(
                    "Benchmark fixture reads "
                            + owner.getName()
                            + "#"
                            + name
                            + ", which no longer exists; update the fixture to the engine change",
                    e);
        }
    }
}
