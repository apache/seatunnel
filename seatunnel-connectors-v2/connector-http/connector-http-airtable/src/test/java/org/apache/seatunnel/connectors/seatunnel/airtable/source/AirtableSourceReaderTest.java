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

package org.apache.seatunnel.connectors.seatunnel.airtable.source;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.source.SimpleTextDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AirtableSourceReaderTest {

    @Mock private SingleSplitReaderContext context;
    @Mock private HttpClientProvider httpClient;

    private HttpParameter parameter;
    private SimpleTextDeserializationSchema schema;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        parameter = new HttpParameter();
        parameter.setUrl("https://api.airtable.com/v0/appBase/table/listRecords");
        parameter.setMethod(HttpRequestMethod.POST);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"content"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        schema = new SimpleTextDeserializationSchema(rowType);
    }

    private AirtableSourceReader createReader(int rateLimitMaxRetries) {
        AirtableSourceReader reader =
                new AirtableSourceReader(
                        parameter, context, schema, null, null, null, 0, 0, rateLimitMaxRetries);
        reader.setHttpClient(httpClient);
        return reader;
    }

    @Test
    public void testRetryOn429ThenSuccess() throws Exception {
        when(httpClient.execute(anyString(), anyString(), any(), any(), any(), anyBoolean()))
                .thenReturn(new HttpResponse(429, "{\"error\":{\"type\":\"RATE_LIMIT\"}}"))
                .thenReturn(
                        new HttpResponse(
                                200,
                                "{\"records\":[{\"id\":\"rec1\",\"fields\":{\"Name\":\"Alice\"}}]}"));

        AirtableSourceReader reader = createReader(2);
        HttpResponse response = reader.executeRequest();

        Assertions.assertEquals(200, response.getCode());
        verify(httpClient, times(2))
                .execute(anyString(), anyString(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void testStopRetryAfterMaxRetries() throws Exception {
        when(httpClient.execute(anyString(), anyString(), any(), any(), any(), anyBoolean()))
                .thenReturn(new HttpResponse(429, "{\"error\":{\"type\":\"RATE_LIMIT\"}}"));

        AirtableSourceReader reader = createReader(1);
        HttpResponse response = reader.executeRequest();

        Assertions.assertEquals(429, response.getCode());
        // 1 initial + 1 retry = 2 calls
        verify(httpClient, times(2))
                .execute(anyString(), anyString(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void testBackoffIsZeroWhenDisabled() {
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, 0, 3);

        Assertions.assertEquals(0L, reader.calculateBackoffMillis(1));
        Assertions.assertEquals(0L, reader.calculateBackoffMillis(5));
    }

    @Test
    public void testBackoffNeverShorterThanConfigured() {
        int base = 1000;
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, base, 5);

        for (int retry = 1; retry <= 5; retry++) {
            long base_ = (long) base * (1L << (retry - 1));
            long ceiling = Math.min(2 * base_, 300000L);
            for (int i = 0; i < 200; i++) {
                long actual = reader.calculateBackoffMillis(retry);
                Assertions.assertTrue(
                        actual >= base_ && actual <= ceiling,
                        "retry "
                                + retry
                                + " produced "
                                + actual
                                + ", expected ["
                                + base_
                                + ", "
                                + ceiling
                                + "]");
            }
        }
    }

    @Test
    public void testBackoffIsNotDeterministic() {
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, 1000, 3);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            observed.add(reader.calculateBackoffMillis(3));
        }

        // Without jitter every call returns the same value. At retry 3 with a
        // 1000ms base the range is [4000, 8000], so seeing a single value across
        // 200 draws is not chance.
        Assertions.assertTrue(
                observed.size() > 1,
                "backoff must vary between calls so that concurrent clients do not "
                        + "retry in lockstep, but observed only "
                        + observed);
    }

    @Test
    public void testBackoffRespectsMaximum() {
        int base = 60000;
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, base, 30);

        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(
                    reader.calculateBackoffMillis(20) <= 300000L,
                    "jittered backoff must never exceed MAX_BACKOFF_MILLIS");
        }
    }

    @Test
    public void testBackoffStillVariesAtTheMaximum() {
        int base = 60000;
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, base, 30);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            observed.add(reader.calculateBackoffMillis(20));
        }

        // There is no headroom to add into once the wait reaches the maximum, so
        // the spread has to go downwards. Leaving it flat would put every caller
        // back in lockstep for exactly the retries that matter most.
        Assertions.assertTrue(
                observed.size() > 1,
                "backoff must still vary once it reaches MAX_BACKOFF_MILLIS, but "
                        + "observed only "
                        + observed);
    }

    @Test
    public void testBackoffMinimumNeverDropsAtTheCapBoundary() {
        // The scheduled wait doubles until it hits the cap. Half the cap can be
        // less than the wait the previous retry already guaranteed, so without a
        // floor the first capped retry could sleep for less than the one before
        // it, which is not what anyone expects from exponential backoff.
        int base = 100000;
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, base, 30);

        // With a 100000ms base the schedule is 100000, 200000, then capped at
        // 300000. The last wait that fitted under the cap was 200000, so no
        // capped retry should ever draw below that. Half the cap, 150000, would.
        for (int retry = 3; retry <= 8; retry++) {
            for (int i = 0; i < 200; i++) {
                long actual = reader.calculateBackoffMillis(retry);
                Assertions.assertTrue(
                        actual >= 200000L && actual <= 300000L,
                        "retry " + retry + " produced " + actual + ", expected [200000, 300000]");
            }
        }
    }

    @Test
    public void testBackoffStillVariesWhenBaseExceedsTheMaximum() {
        // A base at or above MAX_BACKOFF_MILLIS pins waitMillis to the cap from
        // the first retry. Flooring at the base would then leave no room to
        // jitter, which is the lockstep this change exists to remove, and it
        // would bite the operators running the most conservative backoff.
        int base = 300000;
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, base, 30);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            long actual = reader.calculateBackoffMillis(1);
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

    @Test
    public void testBackoffAtTheMaximumNeverDropsBelowConfiguredBase() {
        // A base above half the maximum makes the downward spread collide with
        // the configured backoff, which Airtable's 429 handling depends on.
        int base = 200000;
        AirtableSourceReader reader =
                new AirtableSourceReader(parameter, context, schema, null, null, null, 0, base, 30);

        for (int i = 0; i < 200; i++) {
            long actual = reader.calculateBackoffMillis(20);
            Assertions.assertTrue(
                    actual >= base && actual <= 300000L,
                    "backoff at the maximum produced "
                            + actual
                            + ", expected ["
                            + base
                            + ", 300000]");
        }
    }
}
