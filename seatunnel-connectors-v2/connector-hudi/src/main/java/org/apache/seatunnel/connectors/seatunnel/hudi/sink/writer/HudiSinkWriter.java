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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiSinkState;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HudiSinkWriter implements SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> {

    private final HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

    private transient List<WriteStatus> writeStatusList;

    private final WriteOperationType opType;

    private final Schema schema;

    private final SeaTunnelRowType seaTunnelRowType;

    private final HudiSinkConfig hudiSinkConfig;

    private final List<HoodieRecord<HoodieAvroPayload>> hoodieRecords;

    private final HudiOutputFormat hudiOutputFormat;

    private transient int batchCount = 0;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture<?> scheduledFuture;

    private transient volatile boolean closed = false;

    private transient volatile Exception flushException;


    public HudiSinkWriter(
        SinkWriter.Context context,
        SeaTunnelRowType seaTunnelRowType,
        HudiSinkConfig hudiSinkConfig) throws IOException {

        hudiOutputFormat = new HudiOutputFormat();

        this.hoodieRecords = new ArrayList<>(30);
        this.seaTunnelRowType = seaTunnelRowType;
        this.schema = new Schema.Parser().parse(hudiOutputFormat.convertSchema(seaTunnelRowType));
        this.opType = hudiSinkConfig.getOpType();
        this.hudiSinkConfig = hudiSinkConfig;
        Configuration hadoopConf = HudiUtil.getConfiguration(hudiSinkConfig.getConfFile());

        if (hudiSinkConfig.getBatchIntervalMs() != 0) {
            this.scheduler =
                Executors.newScheduledThreadPool(
                    1,
                    runnable -> {
                        AtomicInteger cnt = new AtomicInteger(0);
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        thread.setName(
                            "hudi-flush" + "-" + cnt.incrementAndGet());
                        return thread;
                    });
            this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                    () -> {
                        synchronized (HudiSinkWriter.this) {
                            if (!closed) {
                                try {
                                    flush();
                                } catch (Exception e) {
                                    flushException = e;
                                }
                            }
                        }
                    },
                    hudiSinkConfig.getBatchIntervalMs(),
                    hudiSinkConfig.getBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }

        // initialize the table, if not done already
        Path path = new Path(hudiSinkConfig.getTablePath());
        FileSystem fs = FSUtils.getFs(hudiSinkConfig.getTablePath(), hadoopConf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(hudiSinkConfig.getTableType())
                .setTableName(hudiSinkConfig.getTableName())
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(hadoopConf, hudiSinkConfig.getTablePath());
        }
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(hudiSinkConfig.getTablePath())
            .withSchema(hudiOutputFormat.convertSchema(seaTunnelRowType)).withParallelism(hudiSinkConfig.getInsertShuffleParallelism(), hudiSinkConfig.getUpsertShuffleParallelism())
            .withDeleteParallelism(hudiSinkConfig.getDeleteShuffleParallelism()).forTable(hudiSinkConfig.getTableName())
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(hudiSinkConfig.getMinCommitsToKeep(), hudiSinkConfig.getMaxCommitsToKeep()).build()).build();


        writeClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkFlushException();

        batchCount++;
        prepareRecords(element);

        if (batchCount >= hudiSinkConfig.getMaxCommitsToKeep()) {
            flush();
        }
    }

    @Override
    public Optional<HudiCommitInfo> prepareCommit() {
        return Optional.of(new HudiCommitInfo(writeStatusList));
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {
       if(!closed){
           if (writeClient != null) {
               writeClient.close();
           }
           closed = true;
           checkFlushException();
       }
    }

    private void prepareRecords(SeaTunnelRow element) {

        hoodieRecords.add(convertRow(element));
    }

    private HoodieRecord<HoodieAvroPayload> convertRow(SeaTunnelRow element){
        //
        HoodieRecord<HoodieAvroPayload> hoodieAvroPayloadHoodieRecord = new HoodieAvroRecord<>();
        return hoodieAvroPayloadHoodieRecord;
    }

    public void flush() {
        checkFlushException();
        switch (opType) {
            case INSERT:
                writeStatusList = writeClient.insert(hoodieRecords, writeClient.startCommit());
            case UPSERT:
                writeStatusList = writeClient.upsert(hoodieRecords, writeClient.startCommit());
        }
        batchCount = 0;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new HudiConnectorException(
                CommonErrorCode.FLUSH_DATA_FAILED,
                "Writing records to Hudi failed.",
                flushException);
        }
    }

}
