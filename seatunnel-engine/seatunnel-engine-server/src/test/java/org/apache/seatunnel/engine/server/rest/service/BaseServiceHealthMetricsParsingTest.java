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

package org.apache.seatunnel.engine.server.rest.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

public class BaseServiceHealthMetricsParsingTest {

    private BaseService baseService;
    private Address memberAddress;
    private Method parseSystemMonitoringMetricsMethod;
    private Method shouldLogInvalidMetricsMethod;

    @BeforeEach
    void setUp() throws Exception {
        baseService = new JobInfoService(org.mockito.Mockito.mock(NodeEngineImpl.class));
        memberAddress = new Address("127.0.0.1", 5801);
        parseSystemMonitoringMetricsMethod =
                BaseService.class.getDeclaredMethod(
                        "parseSystemMonitoringMetrics", String.class, Address.class);
        parseSystemMonitoringMetricsMethod.setAccessible(true);
        shouldLogInvalidMetricsMethod =
                BaseService.class.getDeclaredMethod("shouldLogInvalidMetrics");
        shouldLogInvalidMetricsMethod.setAccessible(true);
    }

    @Test
    void testParseSystemMonitoringMetricsNullInput() throws Exception {
        JsonObject parsed =
                (JsonObject)
                        parseSystemMonitoringMetricsMethod.invoke(baseService, null, memberAddress);
        Assertions.assertEquals(0, parsed.size());
    }

    @Test
    void testParseSystemMonitoringMetricsIgnoreMalformedTokens() throws Exception {
        JsonObject parsed =
                (JsonObject)
                        parseSystemMonitoringMetricsMethod.invoke(
                                baseService, "a=1, broken, =x, c=", memberAddress);
        Assertions.assertEquals("1", parsed.getString("a", null));
        Assertions.assertEquals("", parsed.getString("c", null));
        Assertions.assertNull(parsed.get("broken"));
        Assertions.assertEquals(2, parsed.size());
    }

    @Test
    void testParseSystemMonitoringMetricsKeepCommaInsideValue() throws Exception {
        JsonObject parsed =
                (JsonObject)
                        parseSystemMonitoringMetricsMethod.invoke(
                                baseService,
                                "load.process=12,34%, heap.memory.used=1,2GB, connection.count=10",
                                memberAddress);
        Assertions.assertEquals("12,34%", parsed.getString("load.process", null));
        Assertions.assertEquals("1,2GB", parsed.getString("heap.memory.used", null));
        Assertions.assertEquals("10", parsed.getString("connection.count", null));
    }

    @Test
    void testMalformedMetricsWarningRateLimit() throws Exception {
        long originalLastLogTime = setLastInvalidMetricsLogTimeMs(0L);

        try {
            Assertions.assertTrue((boolean) shouldLogInvalidMetricsMethod.invoke(null));
            Assertions.assertFalse((boolean) shouldLogInvalidMetricsMethod.invoke(null));
        } finally {
            setLastInvalidMetricsLogTimeMs(originalLastLogTime);
        }
    }

    private static long setLastInvalidMetricsLogTimeMs(long value) {
        try {
            Field field = BaseService.class.getDeclaredField("LAST_INVALID_METRICS_LOG_TIME_MS");
            field.setAccessible(true);
            AtomicLong lastInvalidMetricsLogTimeMs = (AtomicLong) field.get(null);
            return lastInvalidMetricsLogTimeMs.getAndSet(value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to set LAST_INVALID_METRICS_LOG_TIME_MS", e);
        }
    }
}
