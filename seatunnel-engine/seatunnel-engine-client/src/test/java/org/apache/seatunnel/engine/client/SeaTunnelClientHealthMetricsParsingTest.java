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
package org.apache.seatunnel.engine.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.logging.ILogger;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SeaTunnelClientHealthMetricsParsingTest {

    @Test
    public void testParseHealthMetricsStringNormal() {
        Map<String, String> parsed = SeaTunnelClient.parseHealthMetricsString("a=1, b=2, c=3");
        Assertions.assertEquals("1", parsed.get("a"));
        Assertions.assertEquals("2", parsed.get("b"));
        Assertions.assertEquals("3", parsed.get("c"));
    }

    @Test
    public void testParseHealthMetricsStringIgnoreMalformedPairs() {
        Map<String, String> parsed =
                SeaTunnelClient.parseHealthMetricsString("a=1, broken, b=2, =x, c=");
        Assertions.assertEquals("1", parsed.get("a"));
        Assertions.assertEquals("2", parsed.get("b"));
        Assertions.assertEquals("", parsed.get("c"));
        Assertions.assertFalse(parsed.containsKey(""));
    }

    @Test
    public void testParseHealthMetricsStringKeepCommaInsideValue() {
        Map<String, String> parsed =
                SeaTunnelClient.parseHealthMetricsString(
                        "load.process=12,34%, heap.memory.used=1,2GB, connection.count=10");
        Assertions.assertEquals("12,34%", parsed.get("load.process"));
        Assertions.assertEquals("1,2GB", parsed.get("heap.memory.used"));
        Assertions.assertEquals("10", parsed.get("connection.count"));
    }

    @Test
    public void testParseHealthMetricsStringNullOrEmpty() {
        Assertions.assertTrue(SeaTunnelClient.parseHealthMetricsString(null).isEmpty());
        Assertions.assertTrue(SeaTunnelClient.parseHealthMetricsString("").isEmpty());
        Assertions.assertTrue(SeaTunnelClient.parseHealthMetricsString("   ").isEmpty());
    }

    @Test
    public void testParseHealthMetricsStringKeepAdditionalEqualsInValue() {
        Map<String, String> parsed =
                SeaTunnelClient.parseHealthMetricsString("token=a=b=c, processors=8");
        Assertions.assertEquals("a=b=c", parsed.get("token"));
        Assertions.assertEquals("8", parsed.get("processors"));
    }

    @Test
    public void testParseHealthMetricsStringLogMalformedTokenWhenLoggerEnabled() {
        long originalLastLogTime = setLastInvalidMetricsLogTimeMs(0L);
        ILogger logger = Mockito.mock(ILogger.class);
        Mockito.when(logger.isWarningEnabled()).thenReturn(true);

        try {
            Map<String, String> parsed =
                    SeaTunnelClient.parseHealthMetricsString(
                            "load.process=12,34%, broken, processors=8", "127.0.0.1:5801", logger);

            Assertions.assertEquals("12,34%", parsed.get("load.process"));
            Assertions.assertEquals("8", parsed.get("processors"));
            Mockito.verify(logger)
                    .warning(Mockito.contains("Invalid zeta health metrics token(s) from member"));
        } finally {
            setLastInvalidMetricsLogTimeMs(originalLastLogTime);
        }
    }

    @Test
    public void testParseHealthMetricsStringWithNullLogger() {
        Map<String, String> parsed =
                SeaTunnelClient.parseHealthMetricsString("a=1, broken, c=", "127.0.0.1:5801", null);
        Assertions.assertEquals("1", parsed.get("a"));
        Assertions.assertEquals("", parsed.get("c"));
    }

    @Test
    public void testParseHealthMetricsStringLogRateLimit() {
        long originalLastLogTime = setLastInvalidMetricsLogTimeMs(0L);
        ILogger logger = Mockito.mock(ILogger.class);
        Mockito.when(logger.isWarningEnabled()).thenReturn(true);

        try {
            SeaTunnelClient.parseHealthMetricsString("broken1, broken2", "127.0.0.1:5801", logger);
            SeaTunnelClient.parseHealthMetricsString("broken3, broken4", "127.0.0.1:5802", logger);
            Mockito.verify(logger, Mockito.times(1)).warning(Mockito.anyString());
        } finally {
            setLastInvalidMetricsLogTimeMs(originalLastLogTime);
        }
    }

    private static long setLastInvalidMetricsLogTimeMs(long value) {
        try {
            Field field =
                    SeaTunnelClient.class.getDeclaredField("LAST_INVALID_METRICS_LOG_TIME_MS");
            field.setAccessible(true);
            AtomicLong lastInvalidMetricsLogTimeMs = (AtomicLong) field.get(null);
            return lastInvalidMetricsLogTimeMs.getAndSet(value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to set LAST_INVALID_METRICS_LOG_TIME_MS", e);
        }
    }
}
