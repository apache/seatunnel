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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitter;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryBatchWriter;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;
import org.apache.seatunnel.connectors.bigquery.sink.writer.TableSchemaUtil;

import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.OutOfRangeException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQuerySinkBatchWriterTest {
    private static final String RESTORED_STREAM =
            "projects/test-project/datasets/test_dataset/tables/test_table/streams/restored";
    private static final String NEW_STREAM =
            "projects/test-project/datasets/test_dataset/tables/test_table/streams/new";

    @Test
    void testAlreadyExistsResponseAdvancesOffsetWithoutRecreatingStream() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFuture(errorResponse(Code.ALREADY_EXISTS)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.flush();

        assertEquals(1, streamWriter.appendCount);
        assertEquals(2, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(0, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testAlreadyExistsExceptionAdvancesOffsetWithoutRecreatingStream() {
        StatusCode statusCode = statusCode(StatusCode.Code.ALREADY_EXISTS);
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFailedFuture(
                                new AlreadyExistsException(
                                        new IOException("append response was lost"),
                                        statusCode,
                                        false)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.flush();

        assertEquals(1, streamWriter.appendCount);
        assertEquals(2, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(0, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testOutOfRangeResponseFailsAndRetainsBuffer() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFuture(errorResponse(Code.OUT_OF_RANGE)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        assertThrows(BigQueryConnectorException.class, sinkWriter::flush);

        assertEquals(1, streamWriter.appendCount);
        assertEquals(0, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(2, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testOutOfRangeExceptionFailsAndRetainsBuffer() {
        StatusCode statusCode = statusCode(StatusCode.Code.OUT_OF_RANGE);
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFailedFuture(
                                new OutOfRangeException(
                                        new IOException("offset is beyond the stream end"),
                                        statusCode,
                                        false)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row");

        assertThrows(BigQueryConnectorException.class, sinkWriter::flush);

        assertEquals(1, streamWriter.appendCount);
        assertEquals(0, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(1, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testCloseDoesNotFlushUncheckpointedRowsInBatchMode() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.close();

        // In batch mode (flushOnClose == false), close() must NOT append un-checkpointed rows
        // to preserve 2PC stream offset consistency during state recovery.
        assertEquals(0, streamWriter.appendCount);
        assertEquals(0, streamWriter.successfulRowCount);
        assertEquals(1, streamWriter.closeCount);
        assertEquals(2, sinkWriter.buffer.length());
    }

    @Test
    void testRestoreFlushesCheckpointedRowsAndSwitchesToNewStream() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class)))
                .thenReturn(FlushRowsResponse.newBuilder().setOffset(99L).build());
        when(client.finalizeWriteStream(RESTORED_STREAM))
                .thenReturn(FinalizeWriteStreamResponse.newBuilder().setRowCount(900L).build());
        when(client.createWriteStream(anyString(), any(WriteStream.class)))
                .thenReturn(WriteStream.newBuilder().setName(NEW_STREAM).build());

        BigQuerySinkBatchWriter sinkWriter = restoreSinkWriter(client, 100L);

        // Rows up to the checkpoint become visible before the old stream is closed off. Rows
        // after it are never flushed, so the replay goes to a new stream from offset 0.
        InOrder order = inOrder(client);
        ArgumentCaptor<FlushRowsRequest> flushRequest =
                ArgumentCaptor.forClass(FlushRowsRequest.class);
        order.verify(client).flushRows(flushRequest.capture());
        order.verify(client).finalizeWriteStream(RESTORED_STREAM);
        order.verify(client).createWriteStream(anyString(), any(WriteStream.class));
        assertEquals(RESTORED_STREAM, flushRequest.getValue().getWriteStream());
        assertEquals(99L, flushRequest.getValue().getOffset().getValue());

        BigQueryBatchWriter batchWriter = (BigQueryBatchWriter) sinkWriter.streamWriter;
        assertEquals(NEW_STREAM, batchWriter.getStreamName());
        assertEquals(0L, batchWriter.getNextOffset());
    }

    @Test
    void testRestoreAcceptsStreamFinalizedByEarlierRestore() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class))).thenThrow(streamFinalized());
        when(client.finalizeWriteStream(RESTORED_STREAM)).thenThrow(streamFinalized());
        when(client.createWriteStream(anyString(), any(WriteStream.class)))
                .thenReturn(WriteStream.newBuilder().setName(NEW_STREAM).build());

        BigQuerySinkBatchWriter sinkWriter = restoreSinkWriter(client, 100L);

        BigQueryBatchWriter batchWriter = (BigQueryBatchWriter) sinkWriter.streamWriter;
        assertEquals(NEW_STREAM, batchWriter.getStreamName());
        assertEquals(0L, batchWriter.getNextOffset());
    }

    @Test
    void testRestoreFailsWhenCheckpointedRowsCannotBeFlushed() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class)))
                .thenThrow(
                        ApiExceptionFactory.createException(
                                new IOException("unavailable"),
                                GrpcStatusCode.of(io.grpc.Status.Code.UNAVAILABLE),
                                true));

        assertThrows(BigQueryConnectorException.class, () -> restoreSinkWriter(client, 100L));

        verify(client, never()).finalizeWriteStream(anyString());
        verify(client, never()).createWriteStream(anyString(), any(WriteStream.class));
    }

    @Test
    void testRestoreFailsWhenFlushDoesNotReachCheckpointOffset() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class)))
                .thenReturn(FlushRowsResponse.newBuilder().setOffset(50L).build());

        assertThrows(BigQueryConnectorException.class, () -> restoreSinkWriter(client, 100L));

        verify(client, never()).finalizeWriteStream(anyString());
        verify(client, never()).createWriteStream(anyString(), any(WriteStream.class));
    }

    @Test
    void testSharedClientIsNotClosedByWriter() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class)))
                .thenReturn(FlushRowsResponse.newBuilder().setOffset(99L).build());
        when(client.finalizeWriteStream(RESTORED_STREAM))
                .thenReturn(FinalizeWriteStreamResponse.getDefaultInstance());
        when(client.createWriteStream(anyString(), any(WriteStream.class)))
                .thenReturn(WriteStream.newBuilder().setName(NEW_STREAM).build());

        restoreSinkWriter(client, 100L).close();

        verify(client, never()).close();
    }

    @Test
    void testBatchWriterWithoutSharedClientOpensItsOwnClient() throws Exception {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.createWriteStream(anyString(), any(WriteStream.class)))
                .thenReturn(WriteStream.newBuilder().setName(NEW_STREAM).build());
        JsonStreamWriter jsonStreamWriter = mock(JsonStreamWriter.class);
        when(jsonStreamWriter.append(any(JSONArray.class), anyLong()))
                .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()));
        BigQuerySinkBatchWriter sinkWriter =
                new BigQuerySinkBatchWriter(testConfig(), null, null, null, 0L);
        sinkWriter.buffer.put("row");

        try (MockedStatic<BigQueryClientFactory> clientFactory = mockClientFactory(client);
                MockedStatic<TableSchemaUtil> ignored = mockTableSchemaUtil(jsonStreamWriter)) {
            sinkWriter.flush();
        }

        verify(jsonStreamWriter).append(any(JSONArray.class), eq(0L));
        BigQueryBatchWriter batchWriter = (BigQueryBatchWriter) sinkWriter.streamWriter;
        assertEquals(NEW_STREAM, batchWriter.getStreamName());
        assertEquals(1L, batchWriter.getNextOffset());

        sinkWriter.close();
        verify(client).close();
    }

    @Test
    void testRestoredWriterWithoutSharedClientRetiresStreamAtFirstCheckpoint() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class)))
                .thenReturn(FlushRowsResponse.newBuilder().setOffset(99L).build());
        when(client.finalizeWriteStream(RESTORED_STREAM))
                .thenReturn(FinalizeWriteStreamResponse.getDefaultInstance());
        when(client.createWriteStream(anyString(), any(WriteStream.class)))
                .thenReturn(WriteStream.newBuilder().setName(NEW_STREAM).build());
        BigQuerySinkBatchWriter sinkWriter =
                new BigQuerySinkBatchWriter(testConfig(), null, null, RESTORED_STREAM, 100L);

        // No row arrives after the restore, so the first barrier has to retire the old stream.
        try (MockedStatic<BigQueryClientFactory> clientFactory = mockClientFactory(client);
                MockedStatic<TableSchemaUtil> ignored =
                        mockTableSchemaUtil(mock(JsonStreamWriter.class))) {
            assertEquals(Optional.empty(), sinkWriter.prepareCommit());
        }

        InOrder order = inOrder(client);
        order.verify(client).flushRows(any(FlushRowsRequest.class));
        order.verify(client).finalizeWriteStream(RESTORED_STREAM);
        order.verify(client).createWriteStream(anyString(), any(WriteStream.class));
    }

    @Test
    void testStreamingWriterWithoutSharedClientDoesNotDropRows() throws Exception {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        JsonStreamWriter jsonStreamWriter = mock(JsonStreamWriter.class);
        when(jsonStreamWriter.append(any(JSONArray.class)))
                .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()));
        BigQuerySinkStreamWriter sinkWriter =
                new BigQuerySinkStreamWriter(testConfig(), null, null);
        sinkWriter.buffer.put("row");

        try (MockedStatic<BigQueryClientFactory> clientFactory = mockClientFactory(client);
                MockedStatic<TableSchemaUtil> ignored = mockTableSchemaUtil(jsonStreamWriter)) {
            sinkWriter.prepareCommit();
        }

        verify(jsonStreamWriter).append(any(JSONArray.class));
        assertEquals(0, sinkWriter.buffer.length());
    }

    @Test
    void testCommitterAcceptsStreamFinalizedByRestore() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class))).thenThrow(streamFinalized());

        // An engine that replays commits on restore can reach the stream after it was retired.
        try (MockedStatic<BigQueryClientFactory> clientFactory = mockClientFactory(client)) {
            new BigQueryCommitter(testConfig())
                    .commit(
                            Collections.singletonList(
                                    new BigQueryCommitInfo(RESTORED_STREAM, 99L)));
        }

        verify(client).flushRows(any(FlushRowsRequest.class));
    }

    @Test
    void testCommitterFailsWhenFlushFails() {
        BigQueryWriteClient client = mock(BigQueryWriteClient.class);
        when(client.flushRows(any(FlushRowsRequest.class)))
                .thenThrow(
                        ApiExceptionFactory.createException(
                                new IOException("unavailable"),
                                GrpcStatusCode.of(io.grpc.Status.Code.UNAVAILABLE),
                                true));
        BigQueryCommitter committer = new BigQueryCommitter(testConfig());

        try (MockedStatic<BigQueryClientFactory> clientFactory = mockClientFactory(client)) {
            assertThrows(
                    BigQueryConnectorException.class,
                    () ->
                            committer.commit(
                                    Collections.singletonList(
                                            new BigQueryCommitInfo(RESTORED_STREAM, 99L))));
        }
    }

    private static BigQuerySinkBatchWriter restoreSinkWriter(
            BigQueryWriteClient client, long restoredNextOffset) {
        BigQuerySinkBatchWriter sinkWriter =
                new BigQuerySinkBatchWriter(
                        testConfig(), null, null, RESTORED_STREAM, restoredNextOffset);

        @SuppressWarnings("unchecked")
        MultiTableResourceManager<BigQueryWriteClient> manager =
                mock(MultiTableResourceManager.class);
        when(manager.getSharedResource()).thenReturn(Optional.of(client));

        try (MockedStatic<TableSchemaUtil> ignored =
                mockTableSchemaUtil(mock(JsonStreamWriter.class))) {
            sinkWriter.setMultiTableResourceManager(manager, 0);
        }
        return sinkWriter;
    }

    private static ReadonlyConfig testConfig() {
        Map<String, Object> options = new HashMap<>();
        options.put(BigQuerySinkOptions.PROJECT_ID.key(), "test-project");
        options.put(BigQuerySinkOptions.DATASET_ID.key(), "test_dataset");
        options.put(BigQuerySinkOptions.TABLE_ID.key(), "test_table");
        return ReadonlyConfig.fromMap(options);
    }

    private static MockedStatic<BigQueryClientFactory> mockClientFactory(
            BigQueryWriteClient client) {
        MockedStatic<BigQueryClientFactory> clientFactory = mockStatic(BigQueryClientFactory.class);
        clientFactory
                .when(() -> BigQueryClientFactory.getWriteClient(any(ReadonlyConfig.class)))
                .thenReturn(client);
        return clientFactory;
    }

    private static MockedStatic<TableSchemaUtil> mockTableSchemaUtil(
            JsonStreamWriter jsonStreamWriter) {
        return mockStatic(
                TableSchemaUtil.class,
                invocation -> {
                    switch (invocation.getMethod().getName()) {
                        case "getActualTableSchema":
                            return TableSchema.getDefaultInstance();
                        case "createStreamWriter":
                            return jsonStreamWriter;
                        default:
                            return invocation.callRealMethod();
                    }
                });
    }

    /** The error a unary call returns when the stream was finalized, as gax surfaces it. */
    private static ApiException streamFinalized() {
        Status status =
                Status.newBuilder()
                        .setCode(Code.INVALID_ARGUMENT.getNumber())
                        .setMessage("Stream is finalized")
                        .addDetails(
                                Any.pack(
                                        StorageError.newBuilder()
                                                .setCode(
                                                        StorageError.StorageErrorCode
                                                                .STREAM_FINALIZED)
                                                .setEntity(RESTORED_STREAM)
                                                .build()))
                        .build();
        return ApiExceptionFactory.createException(
                StatusProto.toStatusRuntimeException(status),
                GrpcStatusCode.of(io.grpc.Status.Code.INVALID_ARGUMENT),
                false);
    }

    private static BigQuerySinkBatchWriter createSinkWriter(BigQueryWriter streamWriter) {
        return new BigQuerySinkBatchWriter(
                ReadonlyConfig.fromMap(Collections.emptyMap()), streamWriter, null);
    }

    private static AppendRowsResponse errorResponse(Code code) {
        return AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(code.getNumber())
                                .setMessage(code.name())
                                .build())
                .build();
    }

    private static StatusCode statusCode(StatusCode.Code code) {
        return new StatusCode() {
            @Override
            public StatusCode.Code getCode() {
                return code;
            }

            @Override
            public Object getTransportCode() {
                return null;
            }
        };
    }

    private static class TestingBigQueryWriter implements BigQueryWriter {
        private final ApiFuture<AppendRowsResponse> result;
        private int appendCount;
        private int successfulRowCount;
        private int closeCount;

        private TestingBigQueryWriter(ApiFuture<AppendRowsResponse> result) {
            this.result = result;
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(JSONArray jsonArr) {
            appendCount++;
            return result;
        }

        @Override
        public void onAppendSuccess(int rowCount) {
            successfulRowCount += rowCount;
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public String getStreamName() {
            return "test-stream";
        }
    }
}
