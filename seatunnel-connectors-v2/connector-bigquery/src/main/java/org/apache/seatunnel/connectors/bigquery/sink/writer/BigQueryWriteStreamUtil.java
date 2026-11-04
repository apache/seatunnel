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

package org.apache.seatunnel.connectors.bigquery.sink.writer;

import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.protobuf.Int64Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryWriteStreamUtil {
    private BigQueryWriteStreamUtil() {}

    /**
     * Makes the rows of a buffered stream visible up to and including {@code flushOffset}.
     *
     * <p>A finalized stream counts as already flushed. The connector finalizes a stream only while
     * retiring it on restore, after flushing it up to the restored checkpoint, and an engine that
     * replays commits on restore can reach the same stream afterwards.
     *
     * @param client write client
     * @param streamName buffered stream to flush
     * @param flushOffset last offset to make visible
     * @throws BigQueryConnectorException if BigQuery reports a flushed offset below {@code
     *     flushOffset}
     */
    public static void flushRows(BigQueryWriteClient client, String streamName, long flushOffset) {
        FlushRowsRequest request =
                FlushRowsRequest.newBuilder()
                        .setWriteStream(streamName)
                        .setOffset(Int64Value.of(flushOffset))
                        .build();

        FlushRowsResponse response;
        try {
            response = client.flushRows(request);
        } catch (RuntimeException e) {
            if (isStreamFinalized(e)) {
                log.info(
                        "BigQuery buffered stream {} was flushed and finalized by an earlier restore",
                        streamName);
                return;
            }
            throw e;
        }

        long flushedOffset = response.getOffset();
        if (flushedOffset < flushOffset) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.COMMIT_FAILED,
                    String.format(
                            "FlushRows did not reach expected offset. stream=%s, expected=%d, actual=%d",
                            streamName, flushOffset, flushedOffset));
        }

        log.info(
                "Successfully flushed BigQuery buffered stream {} to offset {}",
                streamName,
                flushOffset);
    }

    /**
     * Checks whether a failed call reports that the stream is already finalized.
     *
     * @param throwable the failure of a write stream call
     * @return true if BigQuery rejected the call because the stream is finalized
     */
    public static boolean isStreamFinalized(Throwable throwable) {
        return throwable instanceof Exceptions.StreamFinalizedException
                || Exceptions.toStorageException(throwable)
                        instanceof Exceptions.StreamFinalizedException;
    }
}
