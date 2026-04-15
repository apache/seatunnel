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
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryBatchWriter;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class BigQuerySinkBatchWriter extends AbstractBigQuerySinkWriter {
    public static final String BATCH = "batch";

    public BigQuerySinkBatchWriter(
            ReadonlyConfig readOnlyConfig,
            BigQueryWriter streamWriter,
            BigQuerySerializer serializer,
            BigQueryWriteClient client) {
        super(readOnlyConfig, streamWriter, serializer, client);
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
    public List<Void> snapshotState(long checkpointId) {
        this.streamWriter.close();
        this.streamWriter = BigQueryBatchWriter.of(client, config);
        return Collections.emptyList();
    }
}
