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
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.TableName;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamingWriter.DEFAULT_PATH;

@Slf4j
public class BigQueryCommitter implements SinkCommitter<BigQueryCommitInfo> {
    private final ReadonlyConfig config;
    private final String parentTable;

    public BigQueryCommitter(ReadonlyConfig config) {
        this.config = config;
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = config.get(BigQuerySinkOptions.DATASET_ID);
        String tableId = config.get(BigQuerySinkOptions.TABLE_ID);
        this.parentTable = TableName.of(projectId, datasetId, tableId).toString();
    }

    @Override
    public List<BigQueryCommitInfo> commit(List<BigQueryCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.isEmpty()) {
            return commitInfos;
        }

        commitInfos =
                commitInfos.stream()
                        .filter(info -> !info.getStreamName().contains(DEFAULT_PATH))
                        .collect(Collectors.toList());

        if (commitInfos.isEmpty()) {
            return commitInfos;
        }

        try (BigQueryWriteClient client = BigQueryClientFactory.getWriteClient(config)) {
            List<String> streamNames =
                    commitInfos.stream()
                            .map(BigQueryCommitInfo::getStreamName)
                            .collect(Collectors.toList());

            BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(parentTable)
                            .addAllWriteStreams(streamNames)
                            .build();

            BatchCommitWriteStreamsResponse response =
                    client.batchCommitWriteStreams(commitRequest);

            if (response.hasCommitTime()) {
                log.info("Successfully committed {} streams", commitInfos.size());
            } else {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.COMMIT_FAILED,
                        "Commit failed with errors: " + response.getStreamErrorsList());
            }
        } catch (Exception e) {
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.COMMIT_FAILED, e);
        }
        return Collections.emptyList();
    }

    @Override
    public void abort(List<BigQueryCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.isEmpty()) {
            return;
        }

        commitInfos =
                commitInfos.stream()
                        .filter(info -> !info.getStreamName().contains(DEFAULT_PATH))
                        .collect(Collectors.toList());

        if (commitInfos.isEmpty()) {
            return;
        }

        try (BigQueryWriteClient client = BigQueryClientFactory.getWriteClient(config)) {
            for (BigQueryCommitInfo info : commitInfos) {
                try {
                    client.finalizeWriteStream(info.getStreamName());
                    log.info("Successfully finalized (aborted) stream: {}", info.getStreamName());
                } catch (Exception e) {
                    log.error("Failed to explicitly abort stream: {}", info.getStreamName(), e);
                }
            }
        }
    }
}
