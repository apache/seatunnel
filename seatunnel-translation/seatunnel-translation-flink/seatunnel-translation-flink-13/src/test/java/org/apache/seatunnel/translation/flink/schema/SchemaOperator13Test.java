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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.source.SupportSchemaEvolution;

import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaOperator13Test {

    @Test
    void testFallbackTimerAllowsTwoConfiguredCheckpointRounds() {
        assertEquals(75_000L, SchemaOperator13.checkpointStallTimeout(30_000L));
    }

    @Test
    void testFallbackTimerRegistrationIsDeduplicatedAndCanBeRescheduled() throws Exception {
        ProcessingTimeService processingTimeService = Mockito.mock(ProcessingTimeService.class);
        Mockito.when(processingTimeService.getCurrentProcessingTime()).thenReturn(1_000L);
        SchemaOperator13 operator = createOperator();
        operator.setProcessingTimeService(processingTimeService);

        invokeScheduleFallback(operator);
        invokeScheduleFallback(operator);

        ArgumentCaptor<ProcessingTimeCallback> callbackCaptor =
                ArgumentCaptor.forClass(ProcessingTimeCallback.class);
        Mockito.verify(processingTimeService)
                .registerTimer(Mockito.eq(16_000L), callbackCaptor.capture());

        callbackCaptor.getValue().onProcessingTime(16_000L);
        invokeScheduleFallback(operator);

        Mockito.verify(processingTimeService, Mockito.times(2))
                .registerTimer(Mockito.eq(16_000L), Mockito.any(ProcessingTimeCallback.class));
    }

    private static SchemaOperator13 createOperator() throws Exception {
        Class<?> configClass =
                Class.forName("org.apache.seatunnel.shade.com.typesafe.config.Config");
        Object config =
                Proxy.newProxyInstance(
                        configClass.getClassLoader(),
                        new Class<?>[] {configClass},
                        (proxy, method, args) -> null);
        return (SchemaOperator13)
                SchemaOperator13.class
                        .getConstructor(String.class, SupportSchemaEvolution.class, configClass)
                        .newInstance(
                                "schema-operator-13-test",
                                Mockito.mock(SupportSchemaEvolution.class),
                                config);
    }

    private static void invokeScheduleFallback(SchemaOperator13 operator) throws Exception {
        Method method = SchemaOperator13.class.getDeclaredMethod("scheduleFallbackTimer");
        method.setAccessible(true);
        method.invoke(operator);
    }
}
