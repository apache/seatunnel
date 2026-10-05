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

package org.apache.seatunnel.connectors.bigquery.sink.committer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.protobuf.Int64Value;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.DEFAULT_PATH;

@Slf4j
public class BigQueryCommitter implements SinkCommitter<BigQueryCommitInfo> {
    private final ReadonlyConfig config;

    public BigQueryCommitter(ReadonlyConfig config) {
        this.config = config;
    }

    @Override
    public List<BigQueryCommitInfo> commit(List<BigQueryCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.isEmpty()) {
            return commitInfos;
        }

        List<BigQueryCommitInfo> bufferedCommitInfos =
                commitInfos.stream()
                        .filter(Objects::nonNull)
                        .filter(info -> info.getStreamName() != null)
                        .filter(info -> !info.getStreamName().contains(DEFAULT_PATH))
                        .filter(BigQueryCommitInfo::hasData)
                        .collect(Collectors.toList());

        if (bufferedCommitInfos.isEmpty()) {
            return Collections.emptyList();
        }

        try (BigQueryWriteClient client = BigQueryClientFactory.getWriteClient(config)) {
            for (BigQueryCommitInfo info : bufferedCommitInfos) {
                FlushRowsRequest request =
                        FlushRowsRequest.newBuilder()
                                .setWriteStream(info.getStreamName())
                                .setOffset(Int64Value.of(info.getFlushOffset()))
                                .build();

                FlushRowsResponse response = client.flushRows(request);

                long flushedOffset = response.getOffset();
                if (flushedOffset < info.getFlushOffset()) {
                    throw new BigQueryConnectorException(
                            BigQueryConnectorErrorCode.COMMIT_FAILED,
                            String.format(
                                    "FlushRows did not reach expected offset. stream=%s, expected=%d, actual=%d",
                                    info.getStreamName(), info.getFlushOffset(), flushedOffset));
                }

                log.info(
                        "Successfully flushed BigQuery buffered stream {} to offset {}",
                        info.getStreamName(),
                        info.getFlushOffset());
            }
        } catch (Exception e) {
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.COMMIT_FAILED, e);
        }

        return Collections.emptyList();
    }

    @Override
    public void abort(List<BigQueryCommitInfo> commitInfos) {
        // No explicit abort is needed. Unflushed buffered rows are not visible to readers.
    }
}
