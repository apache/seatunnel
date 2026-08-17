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

package org.apache.seatunnel.connectors.seatunnel.splunk.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.splunk.client.SplunkHecClient;
import org.apache.seatunnel.connectors.seatunnel.splunk.client.SplunkHecClient.SplunkHecRetryableException;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

class SplunkSinkWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "message"},
                    new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});

    private static SeaTunnelRow row(long id) {
        return new SeaTunnelRow(new Object[] {id, "message-" + id});
    }

    private static SplunkSinkConfig config(Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://splunk-host:8088");
        options.put("token", "test-token");
        options.put("retry_backoff_ms", 0);
        options.putAll(extraOptions);
        return new SplunkSinkConfig(ReadonlyConfig.fromMap(options));
    }

    private static SplunkSinkConfig configWithBatchSize(int batchSize) {
        Map<String, Object> options = new HashMap<>();
        options.put("max_batch_size", batchSize);
        return config(options);
    }

    @Test
    void batchIsNotSentBeforeTheThresholdIsReached() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        SplunkSinkWriter writer =
                new SplunkSinkWriter(ROW_TYPE, configWithBatchSize(3), null, client);

        writer.write(row(1));
        writer.write(row(2));

        Mockito.verify(client, Mockito.never()).send(Mockito.anyString());
    }

    @Test
    void batchIsSentExactlyWhenTheThresholdIsReached() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        SplunkSinkWriter writer =
                new SplunkSinkWriter(ROW_TYPE, configWithBatchSize(3), null, client);

        writer.write(row(1));
        writer.write(row(2));
        writer.write(row(3));

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(client, Mockito.times(1)).send(body.capture());
        Assertions.assertEquals(3, body.getValue().split("\n").length);
        Assertions.assertTrue(body.getValue().contains("message-1"));
        Assertions.assertTrue(body.getValue().contains("message-3"));

        // The buffer is reset after a successful flush, so the next rows start a fresh batch.
        writer.write(row(4));
        Mockito.verify(client, Mockito.times(1)).send(Mockito.anyString());
    }

    @Test
    void prepareCommitFlushesAPartialBatch() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        SplunkSinkWriter writer =
                new SplunkSinkWriter(ROW_TYPE, configWithBatchSize(100), null, client);

        writer.write(row(1));
        writer.prepareCommit();

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(client, Mockito.times(1)).send(body.capture());
        Assertions.assertEquals(1, body.getValue().split("\n").length);
    }

    @Test
    void closeFlushesAPartialBatchAndClosesTheClient() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        SplunkSinkWriter writer =
                new SplunkSinkWriter(ROW_TYPE, configWithBatchSize(100), null, client);

        writer.write(row(1));
        writer.close();

        Mockito.verify(client, Mockito.times(1)).send(Mockito.anyString());
        Mockito.verify(client, Mockito.times(1)).close();
    }

    @Test
    void flushOnAnEmptyBufferSendsNothing() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        SplunkSinkWriter writer =
                new SplunkSinkWriter(ROW_TYPE, configWithBatchSize(100), null, client);

        writer.prepareCommit();
        writer.close();

        Mockito.verify(client, Mockito.never()).send(Mockito.anyString());
    }

    @Test
    void updateBeforeRowsAreSkipped() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        SplunkSinkWriter writer =
                new SplunkSinkWriter(ROW_TYPE, configWithBatchSize(100), null, client);

        SeaTunnelRow updateBefore = row(1);
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        writer.write(updateBefore);
        writer.prepareCommit();

        Mockito.verify(client, Mockito.never()).send(Mockito.anyString());
    }

    @Test
    void retryableFailuresAreRetriedUpToMaxRetryCount() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        Mockito.doThrow(new SplunkHecRetryableException("collector busy", null))
                .doThrow(new SplunkHecRetryableException("collector busy", null))
                .doNothing()
                .when(client)
                .send(Mockito.anyString());

        Map<String, Object> options = new HashMap<>();
        options.put("max_batch_size", 1);
        options.put("max_retry_count", 3);
        SplunkSinkWriter writer = new SplunkSinkWriter(ROW_TYPE, config(options), null, client);

        writer.write(row(1));

        Mockito.verify(client, Mockito.times(3)).send(Mockito.anyString());
    }

    @Test
    void exhaustedRetriesFailTheTaskAndKeepTheBuffer() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        Mockito.doThrow(new SplunkHecRetryableException("collector busy", null))
                .when(client)
                .send(Mockito.anyString());

        Map<String, Object> options = new HashMap<>();
        options.put("max_batch_size", 1);
        options.put("max_retry_count", 2);
        SplunkSinkWriter writer = new SplunkSinkWriter(ROW_TYPE, config(options), null, client);

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> writer.write(row(1)));
        Assertions.assertEquals(
                SplunkConnectorErrorCode.SEND_EVENTS_FAILED.getCode(),
                exception.getSeaTunnelErrorCode().getCode());
        Mockito.verify(client, Mockito.times(2)).send(Mockito.anyString());

        // The events were never accepted, so they must still be buffered rather than dropped.
        Mockito.clearInvocations(client);
        Mockito.doNothing().when(client).send(Mockito.anyString());
        writer.prepareCommit();

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(client, Mockito.times(1)).send(body.capture());
        Assertions.assertTrue(body.getValue().contains("message-1"));
    }

    @Test
    void permanentFailuresAreNotRetried() throws Exception {
        SplunkHecClient client = Mockito.mock(SplunkHecClient.class);
        Mockito.doThrow(
                        new SplunkConnectorException(
                                SplunkConnectorErrorCode.SEND_EVENTS_FAILED, "invalid token"))
                .when(client)
                .send(Mockito.anyString());

        Map<String, Object> options = new HashMap<>();
        options.put("max_batch_size", 1);
        options.put("max_retry_count", 5);
        SplunkSinkWriter writer = new SplunkSinkWriter(ROW_TYPE, config(options), null, client);

        SplunkConnectorException exception =
                Assertions.assertThrows(SplunkConnectorException.class, () -> writer.write(row(1)));
        Mockito.verify(client, Mockito.times(1)).send(Mockito.anyString());

        // The batch was never retried, so the message must not claim an attempt count.
        Assertions.assertTrue(
                exception.getMessage().contains("rejected it permanently, so it was not retried"),
                exception.getMessage());
        Assertions.assertFalse(
                exception.getMessage().contains("attempt(s)"), exception.getMessage());
    }
}
