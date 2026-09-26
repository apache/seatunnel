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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.connectors.seatunnel.kafka.KafkaClientUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Properties;

class KafkaSinkCommitterTest {

    @Test
    void commitRunsWithConnectorClassLoaderAndRestoresContext() throws Exception {
        assertCommitOperationUsesConnectorClassLoader(true);
    }

    @Test
    void abortRunsWithConnectorClassLoaderAndRestoresContext() throws Exception {
        assertCommitOperationUsesConnectorClassLoader(false);
    }

    private void assertCommitOperationUsesConnectorClassLoader(boolean commit) throws Exception {
        ClassLoader connectorClassLoader = KafkaClientUtils.class.getClassLoader();
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        ClassLoader engineClassLoader = new ClassLoader(originalClassLoader) {};
        KafkaInternalProducer<?, ?> producer = Mockito.mock(KafkaInternalProducer.class);
        java.util.concurrent.atomic.AtomicReference<ClassLoader> observedClassLoader =
                new java.util.concurrent.atomic.AtomicReference<>();
        if (commit) {
            Mockito.doAnswer(
                            invocation -> {
                                observedClassLoader.set(thread.getContextClassLoader());
                                return null;
                            })
                    .when(producer)
                    .commitTransaction();
        } else {
            Mockito.doAnswer(
                            invocation -> {
                                observedClassLoader.set(thread.getContextClassLoader());
                                return null;
                            })
                    .when(producer)
                    .abortTransaction();
        }
        Mockito.doAnswer(
                        invocation -> {
                            observedClassLoader.set(thread.getContextClassLoader());
                            return null;
                        })
                .when(producer)
                .close();

        KafkaSinkCommitter committer = new KafkaSinkCommitter(null);
        java.lang.reflect.Field producerField =
                KafkaSinkCommitter.class.getDeclaredField("kafkaProducer");
        producerField.setAccessible(true);
        producerField.set(committer, producer);
        KafkaCommitInfo commitInfo =
                new KafkaCommitInfo("transaction-1", new Properties(), 1L, (short) 0, false);

        try {
            thread.setContextClassLoader(engineClassLoader);
            if (commit) {
                committer.commit(Collections.singletonList(commitInfo));
            } else {
                committer.abort(Collections.singletonList(commitInfo));
            }
            Assertions.assertSame(connectorClassLoader, observedClassLoader.get());
            Assertions.assertSame(engineClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
        }
    }

    @Test
    void createsCommitProducerWithConnectorClassLoader() throws Exception {
        ClassLoader connectorClassLoader = KafkaClientUtils.class.getClassLoader();
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        ClassLoader engineClassLoader =
                new ClassLoader(originalClassLoader) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (ByteArraySerializer.class.getName().equals(name)) {
                            throw new ClassNotFoundException(name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        kafkaProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaCommitInfo commitInfo =
                new KafkaCommitInfo("transaction-1", kafkaProperties, 1L, (short) 0, false);

        try {
            thread.setContextClassLoader(engineClassLoader);
            KafkaSinkCommitter committer = new KafkaSinkCommitter(null);
            Method getProducer =
                    KafkaSinkCommitter.class.getDeclaredMethod(
                            "getProducer", KafkaCommitInfo.class);
            getProducer.setAccessible(true);

            Object producer =
                    Assertions.assertDoesNotThrow(
                            () -> {
                                try {
                                    return getProducer.invoke(committer, commitInfo);
                                } catch (InvocationTargetException e) {
                                    throw new RuntimeException(e.getCause());
                                }
                            });
            Assertions.assertNotNull(producer);
            KafkaClientUtils.runWithConnectorClassLoader(
                    () -> ((KafkaInternalProducer<?, ?>) producer).close());
            Assertions.assertSame(engineClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
        }
    }
}
