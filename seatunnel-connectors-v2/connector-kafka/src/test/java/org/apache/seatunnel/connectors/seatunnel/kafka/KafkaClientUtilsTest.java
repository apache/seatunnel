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

package org.apache.seatunnel.connectors.seatunnel.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

class KafkaClientUtilsTest {

    @Test
    void shouldRunActionWithConnectorClassLoaderAndRestoreOriginal() {
        Thread thread = Thread.currentThread();
        ClassLoader previousClassLoader = thread.getContextClassLoader();
        ClassLoader originalClassLoader = new ClassLoader(previousClassLoader) {};
        AtomicReference<ClassLoader> actionClassLoader = new AtomicReference<>();

        try {
            thread.setContextClassLoader(originalClassLoader);

            KafkaClientUtils.runWithConnectorClassLoader(
                    () -> actionClassLoader.set(thread.getContextClassLoader()));

            Assertions.assertSame(KafkaClientUtils.class.getClassLoader(), actionClassLoader.get());
            Assertions.assertSame(originalClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(previousClassLoader);
        }
    }

    @Test
    void shouldRestoreOriginalClassLoaderWhenActionFails() {
        Thread thread = Thread.currentThread();
        ClassLoader previousClassLoader = thread.getContextClassLoader();
        ClassLoader originalClassLoader = new ClassLoader(previousClassLoader) {};

        try {
            thread.setContextClassLoader(originalClassLoader);

            Assertions.assertThrows(
                    IllegalStateException.class,
                    () ->
                            KafkaClientUtils.runWithConnectorClassLoader(
                                    () -> {
                                        throw new IllegalStateException("expected");
                                    }));

            Assertions.assertSame(originalClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(previousClassLoader);
        }
    }
}
