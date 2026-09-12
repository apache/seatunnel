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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.transactionlog;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.scan.Db2SnapshotFetchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2.Db2ChangeRecordEmitter;
import io.debezium.connector.db2.Db2ChangeTable;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.connector.db2.Db2Partition;
import io.debezium.connector.db2.Db2SchemaChangeEventEmitter;
import io.debezium.connector.db2.Lsn;
import io.debezium.connector.db2.TxLogPosition;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeTableResultSet;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.offset.LsnOffset.NO_STOPPING_OFFSET;

public class Db2TransactionLogFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public Db2TransactionLogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        Db2SourceFetchTaskContext sourceFetchContext = (Db2SourceFetchTaskContext) context;
        taskRunning = true;

        TransactionLogSplitReadTask transactionLogSplitReadTask =
                new TransactionLogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getMetadataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        split);

        transactionLogSplitReadTask.execute(
                new TransactionLogSplitChangeEventSourceContext(),
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /**
     * Reads Db2 change tables for an incremental split. Debezium's Db2 streaming source has no hook
     * after each handled LSN, so SeaTunnel keeps this small wrapper to emit the snapshot backfill
     * END watermark as soon as the bounded stop LSN is reached.
     */
    public static class TransactionLogSplitReadTask {

        private static final int COL_COMMIT_LSN = 2;
        private static final int COL_ROW_LSN = 3;
        private static final int COL_OPERATION = 1;
        private static final int COL_DATA = 5;
        private static final Pattern MISSING_CDC_FUNCTION_CHANGES_ERROR =
                Pattern.compile("Invalid object name 'cdc.fn_cdc_get_all_changes_(.*)'\\.");
        private static final Logger LOG =
                LoggerFactory.getLogger(TransactionLogSplitReadTask.class);

        private final Db2ConnectorConfig connectorConfig;
        private final Db2Connection dataConnection;
        private final Db2Connection metadataConnection;
        private final JdbcSourceEventDispatcher<Db2Partition> dispatcher;
        private final ErrorHandler errorHandler;
        private final Clock clock;
        private final Db2DatabaseSchema schema;
        private final Duration pollInterval;
        private final IncrementalSplit lsnSplit;

        public TransactionLogSplitReadTask(
                Db2ConnectorConfig connectorConfig,
                Db2Connection dataConnection,
                Db2Connection metadataConnection,
                JdbcSourceEventDispatcher<Db2Partition> dispatcher,
                ErrorHandler errorHandler,
                Db2DatabaseSchema schema,
                IncrementalSplit lsnSplit) {
            this.connectorConfig = connectorConfig;
            this.dataConnection = dataConnection;
            this.metadataConnection = metadataConnection;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
            this.clock = Clock.system();
            this.schema = schema;
            this.pollInterval = connectorConfig.getPollInterval();
            this.lsnSplit = lsnSplit;
        }

        public void execute(
                ChangeEventSource.ChangeEventSourceContext context,
                Db2Partition partition,
                Db2OffsetContext offsetContext)
                throws InterruptedException {
            if (!connectorConfig.getSnapshotMode().shouldStream()) {
                LOG.info("Streaming is not enabled in current configuration");
                return;
            }

            Metronome metronome = Metronome.sleeper(pollInterval, clock);
            Queue<Db2ChangeTable> schemaChangeCheckpoints =
                    new PriorityQueue<>((x, y) -> x.getStopLsn().compareTo(y.getStopLsn()));
            try {
                AtomicReference<Db2ChangeTable[]> tablesSlot =
                        new AtomicReference<>(getCdcTablesToQuery(partition, offsetContext));
                TxLogPosition lastProcessedPositionOnStart = offsetContext.getChangePosition();
                long lastProcessedEventSerialNoOnStart = offsetContext.getEventSerialNo();
                LOG.info(
                        "Last position recorded in offsets is {}[{}]",
                        lastProcessedPositionOnStart,
                        lastProcessedEventSerialNoOnStart);

                TxLogPosition lastProcessedPosition = lastProcessedPositionOnStart;
                boolean shouldIncreaseFromLsn = offsetContext.isSnapshotCompleted();
                while (context.isRunning()) {
                    Lsn currentMaxLsn = dataConnection.getMaxLsn();
                    if (!currentMaxLsn.isAvailable()) {
                        LOG.warn(
                                "No maximum LSN recorded in Db2. Please ensure that the Db2 capture agent is running");
                        metronome.pause();
                        continue;
                    }
                    if (currentMaxLsn.equals(lastProcessedPosition.getCommitLsn())
                            && shouldIncreaseFromLsn) {
                        metronome.pause();
                        continue;
                    }

                    Lsn fromLsn =
                            lastProcessedPosition.getCommitLsn().isAvailable()
                                            && shouldIncreaseFromLsn
                                    ? dataConnection.incrementLsn(
                                            lastProcessedPosition.getCommitLsn())
                                    : lastProcessedPosition.getCommitLsn();
                    shouldIncreaseFromLsn = true;

                    while (!schemaChangeCheckpoints.isEmpty()) {
                        migrateTable(partition, offsetContext, schemaChangeCheckpoints);
                    }
                    if (!dataConnection.listOfNewChangeTables(fromLsn, currentMaxLsn).isEmpty()) {
                        Db2ChangeTable[] tables = getCdcTablesToQuery(partition, offsetContext);
                        tablesSlot.set(tables);
                        for (Db2ChangeTable table : tables) {
                            if (table.getStartLsn().isBetween(fromLsn, currentMaxLsn)) {
                                LOG.info("Schema will be changed for {}", table);
                                schemaChangeCheckpoints.add(table);
                            }
                        }
                    }

                    AtomicBoolean boundedReadFinished = new AtomicBoolean(false);
                    try {
                        dataConnection.getChangesForTables(
                                tablesSlot.get(),
                                fromLsn,
                                currentMaxLsn,
                                resultSets -> {
                                    readChangeTableResultSets(
                                            context,
                                            partition,
                                            offsetContext,
                                            schemaChangeCheckpoints,
                                            tablesSlot,
                                            lastProcessedPositionOnStart,
                                            lastProcessedEventSerialNoOnStart,
                                            resultSets,
                                            boundedReadFinished);
                                });
                        lastProcessedPosition = TxLogPosition.valueOf(currentMaxLsn);
                        dataConnection.rollback();
                        if (boundedReadFinished.get()) {
                            return;
                        }
                    } catch (SQLException e) {
                        tablesSlot.set(processErrorFromChangeTableQuery(e, tablesSlot.get()));
                    }
                }
            } catch (Exception e) {
                errorHandler.setProducerThrowable(e);
            }
        }

        private void readChangeTableResultSets(
                ChangeEventSource.ChangeEventSourceContext context,
                Db2Partition partition,
                Db2OffsetContext offsetContext,
                Queue<Db2ChangeTable> schemaChangeCheckpoints,
                AtomicReference<Db2ChangeTable[]> tablesSlot,
                TxLogPosition lastProcessedPositionOnStart,
                long lastProcessedEventSerialNoOnStart,
                ResultSet[] resultSets,
                AtomicBoolean boundedReadFinished)
                throws SQLException, InterruptedException {
            long eventSerialNoInInitialTx = 1;
            int tableCount = resultSets.length;
            ChangeTablePointer[] changeTables = new ChangeTablePointer[tableCount];
            Db2ChangeTable[] tables = tablesSlot.get();

            for (int i = 0; i < tableCount; i++) {
                changeTables[i] = new ChangeTablePointer(tables[i], resultSets[i]);
                changeTables[i].next();
            }

            for (; ; ) {
                ChangeTablePointer tableWithSmallestLsn = getTableWithSmallestLsn(changeTables);
                if (tableWithSmallestLsn == null || boundedReadFinished.get()) {
                    break;
                }

                if (!tableWithSmallestLsn.getChangePosition().isAvailable()
                        || !tableWithSmallestLsn.getChangePosition().getInTxLsn().isAvailable()) {
                    LOG.error(
                            "Skipping change {} as its LSN is NULL which is not expected",
                            tableWithSmallestLsn);
                    tableWithSmallestLsn.next();
                    continue;
                }
                if (tableWithSmallestLsn.getChangePosition().compareTo(lastProcessedPositionOnStart)
                        < 0) {
                    tableWithSmallestLsn.next();
                    continue;
                }
                if (tableWithSmallestLsn.getChangePosition().compareTo(lastProcessedPositionOnStart)
                                == 0
                        && eventSerialNoInInitialTx <= lastProcessedEventSerialNoOnStart) {
                    eventSerialNoInInitialTx++;
                    tableWithSmallestLsn.next();
                    continue;
                }
                if (tableWithSmallestLsn.getChangeTable().getStopLsn().isAvailable()
                        && tableWithSmallestLsn
                                        .getChangeTable()
                                        .getStopLsn()
                                        .compareTo(
                                                tableWithSmallestLsn
                                                        .getChangePosition()
                                                        .getCommitLsn())
                                <= 0) {
                    tableWithSmallestLsn.next();
                    continue;
                }
                if (!schemaChangeCheckpoints.isEmpty()
                        && tableWithSmallestLsn
                                        .getChangePosition()
                                        .getCommitLsn()
                                        .compareTo(schemaChangeCheckpoints.peek().getStopLsn())
                                >= 0) {
                    migrateTable(partition, offsetContext, schemaChangeCheckpoints);
                }

                TableId tableId = tableWithSmallestLsn.getChangeTable().getSourceTableId();
                int operation = tableWithSmallestLsn.getOperation();
                Object[] data = tableWithSmallestLsn.getData();

                int eventCount = 1;
                if (operation == Db2ChangeRecordEmitter.OP_UPDATE_BEFORE) {
                    if (!tableWithSmallestLsn.next()
                            || tableWithSmallestLsn.getOperation()
                                    != Db2ChangeRecordEmitter.OP_UPDATE_AFTER) {
                        throw new IllegalStateException(
                                "The update before event at "
                                        + tableWithSmallestLsn.getChangePosition()
                                        + " for table "
                                        + tableId
                                        + " was not followed by after event.");
                    }
                    eventCount = 2;
                }
                Object[] dataNext =
                        operation == Db2ChangeRecordEmitter.OP_UPDATE_BEFORE
                                ? tableWithSmallestLsn.getData()
                                : null;

                TxLogPosition currentPosition = tableWithSmallestLsn.getChangePosition();
                offsetContext.setChangePosition(currentPosition, eventCount);
                offsetContext.event(
                        tableId, metadataConnection.timestampOfLsn(currentPosition.getCommitLsn()));

                dispatcher.dispatchDataChangeEvent(
                        partition,
                        tableId,
                        new Db2ChangeRecordEmitter(
                                partition, offsetContext, operation, data, dataNext, clock));

                if (isBoundedRead()
                        && currentLsnOffset(currentPosition)
                                .isAtOrAfter(lsnSplit.getStopOffset())) {
                    dispatchBoundedReadEndEvent(partition, currentPosition, context);
                    boundedReadFinished.set(true);
                    break;
                }
                tableWithSmallestLsn.next();
            }
        }

        private ChangeTablePointer getTableWithSmallestLsn(ChangeTablePointer[] changeTables)
                throws SQLException {
            ChangeTablePointer tableWithSmallestLsn = null;
            for (ChangeTablePointer changeTable : changeTables) {
                if (changeTable.isCompleted()) {
                    continue;
                }
                if (tableWithSmallestLsn == null
                        || changeTable.compareTo(tableWithSmallestLsn) < 0) {
                    tableWithSmallestLsn = changeTable;
                }
            }
            return tableWithSmallestLsn;
        }

        private void migrateTable(
                Db2Partition partition,
                Db2OffsetContext offsetContext,
                Queue<Db2ChangeTable> schemaChangeCheckpoints)
                throws InterruptedException, SQLException {
            Db2ChangeTable newTable = schemaChangeCheckpoints.poll();
            LOG.info("Migrating schema to {}", newTable);
            dispatcher.dispatchSchemaChangeEvent(
                    partition,
                    newTable.getSourceTableId(),
                    new Db2SchemaChangeEventEmitter(
                            partition,
                            offsetContext,
                            newTable,
                            metadataConnection.getTableSchemaFromTable(newTable),
                            SchemaChangeEventType.ALTER));
        }

        private Db2ChangeTable[] processErrorFromChangeTableQuery(
                SQLException exception, Db2ChangeTable[] currentChangeTables) throws SQLException {
            Matcher matcher = MISSING_CDC_FUNCTION_CHANGES_ERROR.matcher(exception.getMessage());
            if (matcher.matches()) {
                String captureName = matcher.group(1);
                LOG.info("Table is no longer captured with capture instance {}", captureName);
                return Arrays.stream(currentChangeTables)
                        .filter(
                                changeTable ->
                                        !changeTable.getCaptureInstance().equals(captureName))
                        .collect(Collectors.toList())
                        .toArray(new Db2ChangeTable[0]);
            }
            throw exception;
        }

        private Db2ChangeTable[] getCdcTablesToQuery(
                Db2Partition partition, Db2OffsetContext offsetContext)
                throws SQLException, InterruptedException {
            Set<Db2ChangeTable> cdcEnabledTables = dataConnection.listOfChangeTables();
            if (cdcEnabledTables.isEmpty()) {
                LOG.warn(
                        "No table has enabled CDC or security constraints prevent getting the list of change tables");
            }

            Map<TableId, List<Db2ChangeTable>> includedAndCdcEnabledTables =
                    cdcEnabledTables.stream()
                            .filter(
                                    changeTable ->
                                            connectorConfig
                                                    .getTableFilters()
                                                    .dataCollectionFilter()
                                                    .isIncluded(changeTable.getSourceTableId()))
                            .collect(Collectors.groupingBy(Db2ChangeTable::getSourceTableId));

            if (includedAndCdcEnabledTables.isEmpty()) {
                LOG.warn(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING);
            }

            List<Db2ChangeTable> tables = new ArrayList<>();
            for (List<Db2ChangeTable> captures : includedAndCdcEnabledTables.values()) {
                Db2ChangeTable currentTable = captures.get(0);
                if (captures.size() > 1) {
                    Db2ChangeTable futureTable;
                    if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn())
                            < 0) {
                        futureTable = captures.get(1);
                    } else {
                        currentTable = captures.get(1);
                        futureTable = captures.get(0);
                    }
                    currentTable.setStopLsn(futureTable.getStartLsn());
                    tables.add(futureTable);
                    LOG.info(
                            "Multiple capture instances present for the same table: {} and {}",
                            currentTable,
                            futureTable);
                }
                if (schema.tableFor(currentTable.getSourceTableId()) == null) {
                    LOG.info(
                            "Table {} is new to be monitored by capture instance {}",
                            currentTable.getSourceTableId(),
                            currentTable.getCaptureInstance());
                    offsetContext.event(currentTable.getSourceTableId(), Instant.now());
                    dispatcher.dispatchSchemaChangeEvent(
                            partition,
                            currentTable.getSourceTableId(),
                            new Db2SchemaChangeEventEmitter(
                                    partition,
                                    offsetContext,
                                    currentTable,
                                    dataConnection.getTableSchemaFromTable(currentTable),
                                    SchemaChangeEventType.CREATE));
                }
                tables.add(currentTable);
            }
            return tables.toArray(new Db2ChangeTable[tables.size()]);
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(lsnSplit.getStopOffset());
        }

        private LsnOffset currentLsnOffset(TxLogPosition position) {
            return LsnOffset.valueOf(
                    position.getCommitLsn().toString(), position.getInTxLsn().toString());
        }

        private void dispatchBoundedReadEndEvent(
                Db2Partition partition,
                TxLogPosition currentPosition,
                ChangeEventSource.ChangeEventSourceContext context)
                throws InterruptedException {
            LsnOffset currentOffset = currentLsnOffset(currentPosition);
            dispatcher.dispatchWatermarkEvent(
                    partition.getSourcePartition(), lsnSplit, currentOffset, WatermarkKind.END);
            if (context
                    instanceof Db2SnapshotFetchTask.SnapshotBinlogSplitChangeEventSourceContext) {
                ((Db2SnapshotFetchTask.SnapshotBinlogSplitChangeEventSourceContext) context)
                        .finished();
            }
        }

        private static class ChangeTablePointer
                extends ChangeTableResultSet<Db2ChangeTable, TxLogPosition> {

            private ChangeTablePointer(Db2ChangeTable changeTable, ResultSet resultSet) {
                super(changeTable, resultSet, COL_DATA);
            }

            @Override
            protected int getOperation(ResultSet resultSet) throws SQLException {
                return resultSet.getInt(COL_OPERATION);
            }

            @Override
            protected TxLogPosition getNextChangePosition(ResultSet resultSet) throws SQLException {
                return isCompleted()
                        ? TxLogPosition.NULL
                        : TxLogPosition.valueOf(
                                Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)),
                                Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
            }
        }
    }

    private class TransactionLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
