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

package org.apache.seatunnel.e2e.common.container;

/**
 * A test container that can remove test-class state without stopping the underlying service.
 *
 * <p>Implementations must be configuration-independent across test classes because the first
 * instance is shared with subsequent classes. Per-class setup must be performed in {@link
 * #prepareForTestClass()} or through {@link TestContainer#executeExtraCommands} rather than during
 * construction or startup. Implementations must detect filesystem or classloader inputs that cannot
 * be restored safely and fail cleanup so the shared resource is restarted. In particular, a class
 * must not opt into reuse when its setup replaces an existing same-path connector artifact or
 * mutates runtime libraries unless the implementation explicitly restores and invalidates that
 * state.
 */
public interface ReusableTestContainer extends TestContainer {

    /** Records or verifies the clean baseline before a test class uses this container. */
    default void prepareForTestClass() throws Exception {}

    /** Removes state owned by the completed test class and verifies the clean baseline. */
    void cleanUpAfterTestClass() throws Exception;
}
