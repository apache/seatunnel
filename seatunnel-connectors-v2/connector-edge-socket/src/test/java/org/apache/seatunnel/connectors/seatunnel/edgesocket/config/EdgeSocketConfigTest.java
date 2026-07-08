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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketPacketMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

class EdgeSocketConfigTest {

    @Test
    void validMinimalConfig() {
        EdgeSocketConfig config = createConfig(minimalConfig());
        Assertions.assertEquals(9999, config.getPort());
        Assertions.assertEquals(EdgeSocketPacketMode.RAW, config.getPacketMode());
        Assertions.assertNull(config.getEndpoint());
        Assertions.assertNull(config.getSecretKey());
    }

    @Test
    void validEndpointParsed() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketCommonOptions.ENDPOINT.key(), "collector.example.com:8080");
        EdgeSocketConfig config = createConfig(map);
        Assertions.assertEquals("collector.example.com:8080", config.getEndpoint());
    }

    @Test
    void endpointWithoutPortThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketCommonOptions.ENDPOINT.key(), "no-port-host");
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("expected format host:port"));
    }

    @Test
    void endpointWithNonNumericPortThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketCommonOptions.ENDPOINT.key(), "host:abc");
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("Invalid endpoint port"));
    }

    @Test
    void endpointColonOnlyThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketCommonOptions.ENDPOINT.key(), ":8080");
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("expected format host:port"));
    }

    @Test
    void endpointTrailingColonThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketCommonOptions.ENDPOINT.key(), "host:");
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("expected format host:port"));
    }

    @Test
    void blankEndpointTreatedAsNull() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketCommonOptions.ENDPOINT.key(), "   ");
        EdgeSocketConfig config = createConfig(map);
        Assertions.assertNull(config.getEndpoint());
    }

    @Test
    void localQueueCapacityZeroThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 0);
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("local_queue_capacity"));
    }

    @Test
    void localQueueCapacityNegativeThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), -5);
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("greater than 0"));
    }

    @Test
    void validSecretKey32Bytes() {
        byte[] key = "test-secret-key-32-bytes-aes256!".getBytes();
        Assertions.assertEquals(32, key.length);
        String base64Key = Base64.getEncoder().encodeToString(key);

        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        map.put(EdgeSocketSourceOptions.SECRET_KEY.key(), base64Key);
        EdgeSocketConfig config = createConfig(map);

        Assertions.assertNotNull(config.getSecretKeyBytes());
        Assertions.assertEquals(32, config.getSecretKeyBytes().length);
    }

    @Test
    void secretKeyTooShortThrows() {
        byte[] shortKey = "only-16-bytes!!!".getBytes();
        Assertions.assertEquals(16, shortKey.length);
        String base64Key = Base64.getEncoder().encodeToString(shortKey);

        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        map.put(EdgeSocketSourceOptions.SECRET_KEY.key(), base64Key);
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("exactly 32 bytes"));
    }

    @Test
    void secretKeyTooLongThrows() {
        byte[] longKey = "this-key-is-48-bytes-long-and-way-too-big-for-it".getBytes();
        Assertions.assertEquals(48, longKey.length);
        String base64Key = Base64.getEncoder().encodeToString(longKey);

        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        map.put(EdgeSocketSourceOptions.SECRET_KEY.key(), base64Key);
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("exactly 32 bytes"));
    }

    @Test
    void secretKeyIgnoredInRawMode() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "RAW");
        map.put(EdgeSocketSourceOptions.SECRET_KEY.key(), "invalid-not-base64!!!");
        EdgeSocketConfig config = createConfig(map);
        Assertions.assertNull(config.getSecretKeyBytes());
    }

    @Test
    void packetModeCaseInsensitive() {
        byte[] key = "test-secret-key-32-bytes-aes256!".getBytes();
        String base64Key = Base64.getEncoder().encodeToString(key);

        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "packet");
        map.put(EdgeSocketSourceOptions.SECRET_KEY.key(), base64Key);
        EdgeSocketConfig config = createConfig(map);
        Assertions.assertEquals(EdgeSocketPacketMode.PACKET, config.getPacketMode());
    }

    @Test
    void invalidPacketModeThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "UNKNOWN");
        Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
    }

    @Test
    void invalidAuthTypeThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.AUTH_TYPE.key(), "UNSUPPORTED");
        Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
    }

    @Test
    void missingTokenThrowsWhenAuthTypeIsToken() {
        Map<String, Object> map = minimalConfig();
        map.remove(EdgeSocketSourceOptions.TOKEN.key());
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("token is required"));
    }

    @Test
    void defaultBackpressureOptions() {
        EdgeSocketConfig config = createConfig(minimalConfig());
        Assertions.assertEquals(0.9, config.getQueueBackpressureWatermarkRatio());
        Assertions.assertEquals(500, config.getQueueFullRetryAfterMs());
    }

    @Test
    void invalidQueueBackpressureWatermarkRatioThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.QUEUE_BACKPRESSURE_WATERMARK_RATIO.key(), 1.5);
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("queue_backpressure_watermark_ratio"));
    }

    @Test
    void invalidQueueFullRetryAfterMsThrows() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.QUEUE_FULL_RETRY_AFTER_MS.key(), 0);
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("queue_full_retry_after_ms"));
    }

    @Test
    void blankTokenThrowsWhenAuthTypeIsToken() {
        Map<String, Object> map = minimalConfig();
        map.put(EdgeSocketSourceOptions.TOKEN.key(), "   ");
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> createConfig(map));
        Assertions.assertTrue(ex.getMessage().contains("token is required"));
    }

    private static Map<String, Object> minimalConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeSocketCommonOptions.PORT.key(), 9999);
        map.put(EdgeSocketSourceOptions.TOKEN.key(), "test-token");
        return map;
    }

    private static EdgeSocketConfig createConfig(Map<String, Object> map) {
        return new EdgeSocketConfig(ReadonlyConfig.fromMap(map));
    }
}
