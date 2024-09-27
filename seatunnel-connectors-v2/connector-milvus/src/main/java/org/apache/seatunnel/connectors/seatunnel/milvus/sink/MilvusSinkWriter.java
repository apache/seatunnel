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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.batch.MilvusBatchWriter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.batch.MilvusBufferBatchWriter;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.milvus.state.MilvusSinkState;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.BATCH_SIZE;

@Slf4j
/** MilvusSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Milvus. */
public class MilvusSinkWriter
        implements SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState> {
    private final Context context;

    private final ReadonlyConfig config;
    private MilvusBatchWriter batchWriter;

    public MilvusSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            List<MilvusSinkState> milvusSinkStates) {
        this.context = context;
        this.config = config;
        ConnectConfig connectConfig =
                ConnectConfig.builder()
                        .uri(config.get(MilvusSinkConfig.URL))
                        .token(config.get(MilvusSinkConfig.TOKEN))
                        .dbName(config.get(MilvusSinkConfig.DATABASE))
                        .build();
        this.batchWriter =
                new MilvusBufferBatchWriter(
                        catalogTable,
                        config.get(BATCH_SIZE),
                        getAutoId(catalogTable.getTableSchema().getPrimaryKey()),
                        config.get(MilvusSinkConfig.ENABLE_UPSERT),
                        new MilvusClientV2(connectConfig));
    }

    /**
     * write data to third party data receiver.
     *
     * @param element the data need be written.
     * @throws IOException throw IOException when write data failed.
     */
    @Override
    public void write(SeaTunnelRow element) {
        batchWriter.addToBatch(element);
        if (batchWriter.needFlush()) {
            batchWriter.flush();
        }
    }

    private Boolean getAutoId(PrimaryKey primaryKey) {
        if (null != primaryKey && null != primaryKey.getEnableAutoId()) {
            return primaryKey.getEnableAutoId();
        } else {
            return config.get(MilvusSinkConfig.ENABLE_AUTO_ID);
        }
    }

    /**
     * prepare the commit, will be called before {@link #snapshotState(long checkpointId)}. If you
     * need to use 2pc, you can return the commit info in this method, and receive the commit info
     * in {@link SinkCommitter#commit(List)}. If this method failed (by throw exception), **Only**
     * Spark engine will call {@link #abortPrepare()}
     *
     * @return the commit info need to commit
     */
    @Override
    public Optional<MilvusCommitInfo> prepareCommit() throws IOException {
        batchWriter.flush();
        return Optional.empty();
    }

    /**
     * Used to abort the {@link #prepareCommit()}, if the prepareCommit failed, there is no
     * CommitInfoT, so the rollback work cannot be done by {@link SinkCommitter}. But we can use
     * this method to rollback side effects of {@link #prepareCommit()}. Only use it in Spark engine
     * at now.
     */
    @Override
    public void abortPrepare() {}

    /**
     * call it when SinkWriter close
     *
     * @throws IOException if close failed
     */
    @Override
    public void close() throws IOException {
        if (batchWriter != null) {
            batchWriter.flush();
            batchWriter.close();
        }
    }
}
