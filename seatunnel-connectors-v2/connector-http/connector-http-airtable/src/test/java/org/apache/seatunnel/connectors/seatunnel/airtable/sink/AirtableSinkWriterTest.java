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

package org.apache.seatunnel.connectors.seatunnel.airtable.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AirtableSinkWriterTest {

    @Mock private HttpClientProvider httpClient;

    private SeaTunnelRowType rowType;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        rowType =
                new SeaTunnelRowType(
                        new String[] {"Name", "Age"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }

    private AirtableSinkWriter createWriter(int batchSize, boolean typecast) throws Exception {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://api.airtable.com/v0/appXXX/tblYYY");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer test_token");
        headers.put("Content-Type", "application/json");
        param.setHeaders(headers);

        AirtableSinkWriter writer =
                new AirtableSinkWriter(rowType, param, batchSize, typecast, 0, 0, 3);

        Field field = AirtableSinkWriter.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(writer, httpClient);
        return writer;
    }

    @Test
    public void testBatchWriteBodyFormat() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(200, "{}"));

        AirtableSinkWriter writer = createWriter(2, false);
        writer.write(new SeaTunnelRow(new Object[] {"Alice", 30}));
        writer.write(new SeaTunnelRow(new Object[] {"Bob", 25}));

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient, times(1)).doPost(anyString(), any(), bodyCaptor.capture());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(bodyCaptor.getValue());
        Assertions.assertTrue(root.has("records"));
        Assertions.assertFalse(root.has("typecast"));

        JsonNode records = root.get("records");
        Assertions.assertEquals(2, records.size());
        Assertions.assertTrue(records.get(0).has("fields"));
        Assertions.assertEquals("Alice", records.get(0).get("fields").get("Name").asText());
    }

    @Test
    public void testThrowsAfterMaxRetries() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(429, "{\"error\":{\"type\":\"RATE_LIMIT\"}}"));

        AirtableSinkWriter writer = createWriter(1, false);

        Assertions.assertThrows(
                IOException.class,
                () -> writer.write(new SeaTunnelRow(new Object[] {"Alice", 30})));
        // 1 initial + 3 retries = 4 calls
        verify(httpClient, times(4)).doPost(anyString(), any(), anyString());
    }

    private AirtableSinkWriter writerWithBackoff(int rateLimitBackoffMs, int rateLimitMaxRetries) {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://api.airtable.com/v0/appXXX/tblYYY");
        return new AirtableSinkWriter(
                rowType, param, 1, false, 0, rateLimitBackoffMs, rateLimitMaxRetries);
    }

    // calculateBackoffMillis is duplicated verbatim in AirtableSourceReader. These
    // mirror the reader's cases so the two copies cannot drift apart unnoticed.

    @Test
    public void testBackoffIsZeroWhenDisabled() {
        AirtableSinkWriter writer = writerWithBackoff(0, 3);

        Assertions.assertEquals(0L, writer.calculateBackoffMillis(1));
        Assertions.assertEquals(0L, writer.calculateBackoffMillis(5));
    }

    @Test
    public void testBackoffNeverShorterThanConfigured() {
        int base = 1000;
        AirtableSinkWriter writer = writerWithBackoff(base, 5);

        for (int retry = 1; retry <= 5; retry++) {
            long scheduled = (long) base * (1L << (retry - 1));
            long ceiling = Math.min(2 * scheduled, 300000L);
            for (int i = 0; i < 200; i++) {
                long actual = writer.calculateBackoffMillis(retry);
                Assertions.assertTrue(
                        actual >= scheduled && actual <= ceiling,
                        "retry "
                                + retry
                                + " produced "
                                + actual
                                + ", expected ["
                                + scheduled
                                + ", "
                                + ceiling
                                + "]");
            }
        }
    }

    @Test
    public void testBackoffIsNotDeterministic() {
        AirtableSinkWriter writer = writerWithBackoff(1000, 3);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            observed.add(writer.calculateBackoffMillis(3));
        }

        Assertions.assertTrue(
                observed.size() > 1,
                "backoff must vary between calls so that concurrent writers do not "
                        + "retry in lockstep, but observed only "
                        + observed);
    }

    @Test
    public void testBackoffRespectsMaximum() {
        AirtableSinkWriter writer = writerWithBackoff(60000, 30);

        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(
                    writer.calculateBackoffMillis(20) <= 300000L,
                    "jittered backoff must never exceed MAX_BACKOFF_MILLIS");
        }
    }

    @Test
    public void testBackoffStillVariesAtTheMaximum() {
        AirtableSinkWriter writer = writerWithBackoff(60000, 30);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            observed.add(writer.calculateBackoffMillis(20));
        }

        Assertions.assertTrue(
                observed.size() > 1,
                "backoff must still vary once it reaches MAX_BACKOFF_MILLIS, but observed only "
                        + observed);
    }

    @Test
    public void testBackoffAtTheMaximumNeverDropsBelowConfiguredBase() {
        int base = 200000;
        AirtableSinkWriter writer = writerWithBackoff(base, 30);

        for (int i = 0; i < 200; i++) {
            long actual = writer.calculateBackoffMillis(20);
            Assertions.assertTrue(
                    actual >= base && actual <= 300000L,
                    "backoff at the maximum produced "
                            + actual
                            + ", expected ["
                            + base
                            + ", 300000]");
        }
    }

    @Test
    public void testBackoffMinimumNeverDropsAtTheCapBoundary() {
        // The scheduled wait doubles until it hits the cap. Half the cap can be
        // less than the wait the previous retry already guaranteed, so without a
        // floor the first capped retry could sleep for less than the one before
        // it, which is not what anyone expects from exponential backoff.
        int base = 100000;
        AirtableSinkWriter writer = writerWithBackoff(base, 30);

        // With a 100000ms base the schedule is 100000, 200000, then capped at
        // 300000. The last wait that fitted under the cap was 200000, so no
        // capped retry should ever draw below that. Half the cap, 150000, would.
        for (int retry = 3; retry <= 8; retry++) {
            for (int i = 0; i < 200; i++) {
                long actual = writer.calculateBackoffMillis(retry);
                Assertions.assertTrue(
                        actual >= 200000L && actual <= 300000L,
                        "retry " + retry + " produced " + actual + ", expected [200000, 300000]");
            }
        }
    }

    @Test
    public void testBackoffStillVariesWhenBaseExceedsTheMaximum() {
        AirtableSinkWriter writer = writerWithBackoff(300000, 30);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            long actual = writer.calculateBackoffMillis(1);
            Assertions.assertTrue(
                    actual >= 150000L && actual <= 300000L,
                    "backoff produced " + actual + ", expected [150000, 300000]");
            observed.add(actual);
        }

        Assertions.assertTrue(
                observed.size() > 1,
                "backoff must still vary when the configured base is at or above the maximum, "
                        + "but observed only "
                        + observed);
    }
}
