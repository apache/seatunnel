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

package org.apache.seatunnel.connectors.selectdb.sink.writer;

import org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkConfig;
import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorErrorCode;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;
import org.apache.seatunnel.connectors.selectdb.rest.models.RespContent;
import org.apache.seatunnel.connectors.selectdb.schema.SchemaChangeManager;
import org.apache.seatunnel.connectors.selectdb.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.LoadStatus;
import org.apache.seatunnel.connectors.selectdb.serialize.SelectDBSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.util.HttpUtil;
import org.apache.seatunnel.connectors.selectdb.util.UnsupportedTypeConverterUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

@Slf4j
public class SelectDBStreamLoadSinkWriter
        implements SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {

    private static final int INITIAL_DELAY = 200;
    private static final List<String> SELECTDB_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));
    private long lastCheckpointId;
    private SelectDBStreamLoad selectDBStreamLoad;
    private final SelectDBSinkConfig selectDBSinkConfig;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private SelectDBSerializer serializer;
    private final CatalogTable catalogTable;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile Exception loadException = null;
    private TableSchema tableSchema;
    private final TablePath sinkTablePath;
    protected TableSchemaChangeEventDispatcher tableSchemaChanger =
            new TableSchemaChangeEventDispatcher();
    private SchemaChangeManager schemaChangeManager;

    public SelectDBStreamLoadSinkWriter(
            Context context,
            List<SelectDBSinkState> state,
            CatalogTable catalogTable,
            SelectDBSinkConfig selectDBSinkConfig,
            String jobId) {
        this.selectDBSinkConfig = selectDBSinkConfig;
        this.catalogTable = catalogTable;
        this.lastCheckpointId = !state.isEmpty() ? state.get(0).getCheckpointId() : 0;
        log.info("restore checkpointId {}", lastCheckpointId);
        log.info("labelPrefix " + selectDBSinkConfig.getLabelPrefix());
        this.labelPrefix =
                selectDBSinkConfig.getLabelPrefix()
                        + "_"
                        + catalogTable.getTablePath().getFullName().replaceAll("\\.", "_")
                        + "_"
                        + jobId
                        + "_"
                        + context.getIndexOfSubtask();
        this.labelGenerator = new LabelGenerator(labelPrefix, selectDBSinkConfig.getEnable2PC());
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1, new ThreadFactoryBuilder().setNameFormat("stream-load-check").build());
        this.serializer = createSerializer(selectDBSinkConfig, catalogTable.getSeaTunnelRowType());
        this.intervalTime = selectDBSinkConfig.getCheckInterval();
        this.tableSchema = catalogTable.getTableSchema();
        this.sinkTablePath = catalogTable.getTablePath();
        this.schemaChangeManager = new SchemaChangeManager(selectDBSinkConfig);
        this.initializeLoad();
    }

    private void initializeLoad() {
        List<String> feNodes = Arrays.asList(selectDBSinkConfig.getFrontends().split(","));
        Collections.shuffle(feNodes);
        int feNodesNum = feNodes.size();

        for (int i = 0; i <  feNodesNum; i++) {
            try {
                this.selectDBStreamLoad =
                        new SelectDBStreamLoad(
                                feNodes.get(i),
                                catalogTable.getTablePath(),
                                selectDBSinkConfig,
                                labelGenerator,
                                HttpUtil.getHttpRedirectClient());
                if (selectDBSinkConfig.getEnable2PC()) {
                    selectDBStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
                }
                break;
            } catch (Exception e) {
                if (i == feNodesNum - 1) {
                    throw new SelectDBConnectorException(
                            SelectDBConnectorErrorCode.STREAM_LOAD_FAILED, e);
                }
                log.error("stream load error for feNode: {} with exception: {}",
                        feNodes.get(i),
                        e.getMessage());
            }
        }

        startLoad(labelGenerator.generateLabel(lastCheckpointId + 1));
        // when uploading data in streaming mode, we need to regularly detect whether there are
        // exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(
                this::checkDone, INITIAL_DELAY, intervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkLoadException();
        byte[] serialize =
                serializer.serialize(
                        selectDBSinkConfig.isNeedsUnsupportedTypeCasting()
                                ? UnsupportedTypeConverterUtils.convertRow(element)
                                : element);
        if (Objects.isNull(serialize)) {
            return;
        }

        selectDBStreamLoad.writeRecord(serialize);
        if (!selectDBSinkConfig.getEnable2PC()
                && selectDBStreamLoad.getRecordCount() >= selectDBSinkConfig.getBatchSize()) {
            flush();
            startLoad(labelGenerator.generateLabel(lastCheckpointId));
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {

        this.tableSchema = tableSchemaChanger.reset(tableSchema).apply(event);
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(this.selectDBSinkConfig, seaTunnelRowType);

        try {
            schemaChangeManager.applySchemaChange(sinkTablePath, event);
        } catch (Exception e) {
            throw new SelectDBConnectorException(
                    SelectDBConnectorErrorCode.SCHEMA_CHANGE_FAILED, "Failed to schemaChange");
        }
    }

    @Override
    public Optional<SelectDBCommitInfo> prepareCommit() throws IOException {
        RespContent respContent = flush();
        if (!selectDBSinkConfig.getEnable2PC() || respContent == null) {
            return Optional.empty();
        }

        long txnId = respContent.getTxnId();
        return Optional.of(
                new SelectDBCommitInfo(
                        selectDBStreamLoad.getHostPort(), selectDBStreamLoad.getDb(), txnId));
    }

    private RespContent flush() throws IOException {
        // disable exception checker before stop load.
        checkState(selectDBStreamLoad != null);
        RespContent respContent = selectDBStreamLoad.stopLoad();
        if (respContent != null && !SELECTDB_SUCCESS_STATUS.contains(respContent.getStatus())) {
            String errMsg =
                    String.format(
                            "stream load error: %s, see more in %s",
                            respContent.getMessage(), respContent.getErrorURL());
            throw new SelectDBConnectorException(
                    SelectDBConnectorErrorCode.STREAM_LOAD_FAILED, errMsg);
        }
        return respContent;
    }

    @Override
    public List<SelectDBSinkState> snapshotState(long checkpointId) {
        checkState(selectDBStreamLoad != null);
        startLoad(labelGenerator.generateLabel(checkpointId + 1));
        this.lastCheckpointId = checkpointId;
        return Collections.singletonList(new SelectDBSinkState(labelPrefix, lastCheckpointId));
    }

    private void startLoad(String label) {
        this.selectDBStreamLoad.startLoad(label);
    }

    @Override
    public void abortPrepare() {
        if (selectDBSinkConfig.getEnable2PC()) {
            try {
                selectDBStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        String errorMsg;
        log.debug("start timer checker, interval {} ms", intervalTime);
        if ((errorMsg = selectDBStreamLoad.getLoadFailedMsg()) != null) {
            log.error("stream load finished unexpectedly: {}", errorMsg);
            loadException =
                    new SelectDBConnectorException(
                            SelectDBConnectorErrorCode.STREAM_LOAD_FAILED, errorMsg);
        }
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
        }
    }

    @Override
    public void close() throws IOException {
        if (!selectDBSinkConfig.getEnable2PC()) {
            flush();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (selectDBStreamLoad != null) {
            selectDBStreamLoad.close();
        }
    }

    private SelectDBSerializer createSerializer(
            SelectDBSinkConfig selectDBSinkConfig, SeaTunnelRowType seaTunnelRowType) {
        return new SeaTunnelRowSerializer(
                selectDBSinkConfig
                        .getStreamLoadProps()
                        .getProperty(LoadConstants.FORMAT_KEY)
                        .toLowerCase(),
                seaTunnelRowType,
                selectDBSinkConfig.getStreamLoadProps().getProperty(LoadConstants.FIELD_DELIMITER_KEY),
                selectDBSinkConfig.getEnableDelete());
    }
}
