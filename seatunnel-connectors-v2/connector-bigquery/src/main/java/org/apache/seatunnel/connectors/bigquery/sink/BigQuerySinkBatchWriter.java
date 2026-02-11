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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryBatchWriter;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;

import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.TableName;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamingWriter.DEFAULT_PATH;

@Slf4j
public class BigQuerySinkBatchWriter extends AbstractBigQuerySinkWriter {
    public static final String BATCH = "batch";

    public BigQuerySinkBatchWriter(
            ReadonlyConfig readOnlyConfig,
            List<BigQuerySinkState> states,
            BigQueryWriter streamWriter,
            BigQuerySerializer serializer,
            BigQueryWriteClient client) {
        super(readOnlyConfig, states, streamWriter, serializer, client);

        String projectId = readOnlyConfig.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = readOnlyConfig.get(BigQuerySinkOptions.DATASET_ID);
        String tableId = readOnlyConfig.get(BigQuerySinkOptions.TABLE_ID);
        initialize(states, TableName.of(projectId, datasetId, tableId).toString());
    }

    private void initialize(List<BigQuerySinkState> states, String parentTable) {
        if (states == null || states.isEmpty()) {
            return;
        }

        List<BigQuerySinkState> bigQuerySinkStates =
                states.stream()
                        .filter(info -> !info.getStreamName().contains(DEFAULT_PATH))
                        .collect(Collectors.toList());

        try (BigQueryWriteClient client = BigQueryClientFactory.getWriteClient(config)) {
            List<String> streamNames =
                    bigQuerySinkStates.stream()
                            .map(BigQuerySinkState::getStreamName)
                            .collect(Collectors.toList());

            BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(parentTable)
                            .addAllWriteStreams(streamNames)
                            .build();

            BatchCommitWriteStreamsResponse response =
                    client.batchCommitWriteStreams(commitRequest);

            if (response.hasCommitTime()) {
                log.info("Successfully committed {} streams", bigQuerySinkStates.size());
            } else {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.COMMIT_FAILED,
                        "Commit failed with errors: " + response.getStreamErrorsList());
            }
        } catch (Exception e) {
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.COMMIT_FAILED, e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        buffer.put(serializer.convert(element, false));

        if (buffer.length() >= batchSize) {
            flush();
        }
    }

    @Override
    public Optional<BigQueryCommitInfo> prepareCommit() {
        flush();
        streamWriter.finalizeStream();
        return Optional.of(new BigQueryCommitInfo(streamWriter.getStreamName()));
    }

    @Override
    public void abortPrepare() {}

    @Override
    public List<BigQuerySinkState> snapshotState(long checkpointId) {
        String streamName = this.streamWriter.getStreamName();
        this.streamWriter.close();
        this.streamWriter = BigQueryBatchWriter.of(client, config);
        return Collections.singletonList(new BigQuerySinkState(streamName));
    }
}
