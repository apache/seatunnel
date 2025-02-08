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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlUtils;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.GtidUtils;
import io.debezium.connector.mysql.MySqlChangeEventSourceMetricsFactory;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlErrorHandler;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset.BINLOG_FILENAME_OFFSET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils.createBinaryClient;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils.createMySqlConnection;

/** The context for fetch task that fetching data of snapshot split from MySQL data source. */
@Slf4j
public class MySqlSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceFetchTaskContext.class);

    private final MySqlConnection connection;
    private final BinaryLogClient binaryLogClient;
    private final MySqlEventMetadataProvider metadataProvider;
    private MySqlDatabaseSchema databaseSchema;
    private MySqlTaskContextImpl taskContext;
    private MySqlOffsetContext offsetContext;
    private SnapshotChangeEventSourceMetrics<MySqlPartition> snapshotChangeEventSourceMetrics;
    private MySqlStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<MySqlPartition> dispatcher;
    private MySqlPartition mySqlPartition;
    private ChangeEventQueue<DataChangeEvent> queue;
    private MySqlErrorHandler errorHandler;
    private RelationalDatabaseConnectorConfig dbzConnectorConfig;

    public MySqlSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.dbzConnectorConfig = sourceConfig.getDbzConnectorConfig();
        this.connection = createMySqlConnection(sourceConfig.getDbzConfiguration());
        this.binaryLogClient = createBinaryClient(sourceConfig.getDbzConfiguration());
        this.metadataProvider = new MySqlEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        registerDatabaseHistory(sourceSplitBase);

        // initial stateful objects
        final MySqlConnectorConfig connectorConfig = getDbzConnectorConfig();
        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        this.topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);

        this.databaseSchema =
                MySqlConnectionUtils.createMySqlDatabaseSchema(
                        connectorConfig, tableIdCaseInsensitive);
        this.offsetContext =
                loadStartingOffsetState(
                        new MySqlOffsetContext.Loader(connectorConfig), sourceSplitBase);
        this.mySqlPartition = new MySqlPartition(connectorConfig.getLogicalName());

        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext =
                new MySqlTaskContextImpl(connectorConfig, databaseSchema, binaryLogClient);

        // If in the snapshot read phase and enable exactly-once, the queue needs to be set to a
        // maximum size of `Integer.MAX_VALUE` (buffered a current snapshot all data). otherwise,
        // use the configuration queue size.
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "mysql-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        final MySqlChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new MySqlChangeEventSourceMetricsFactory(
                        new MySqlStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider));
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                (MySqlStreamingChangeEventSourceMetrics)
                        changeEventSourceMetricsFactory.getStreamingMetrics(
                                taskContext, queue, metadataProvider);
        this.errorHandler = new MySqlErrorHandler(connectorConfig, queue);
    }

    @Override
    public void close() {
        try {
            this.connection.close();
            this.binaryLogClient.disconnect();
        } catch (SQLException e) {
            log.warn("Failed to close connection", e);
        } catch (IOException e) {
            log.warn("Failed to close binaryLogClient", e);
        }
    }

    @Override
    public MySqlSourceConfig getSourceConfig() {
        return (MySqlSourceConfig) sourceConfig;
    }

    public MySqlConnection getConnection() {
        return connection;
    }

    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }

    public MySqlTaskContextImpl getTaskContext() {
        return taskContext;
    }

    @Override
    public MySqlConnectorConfig getDbzConnectorConfig() {
        return (MySqlConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public MySqlOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public MySqlPartition getPartition() {
        return mySqlPartition;
    }

    public SnapshotChangeEventSourceMetrics<MySqlPartition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public MySqlStreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public MySqlDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return MySqlUtils.getSplitType(table, dbzConnectorConfig);
    }

    @Override
    public JdbcSourceEventDispatcher<MySqlPartition> getDispatcher() {
        return dispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return MySqlUtils.getBinlogPosition(sourceRecord);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private MySqlOffsetContext loadStartingOffsetState(
            MySqlOffsetContext.Loader loader, SourceSplitBase mySqlSplit) {
        Offset offset =
                mySqlSplit.isSnapshotSplit()
                        ? BinlogOffset.INITIAL_OFFSET
                        : mySqlSplit.asIncrementalSplit().getStartupOffset();

        MySqlOffsetContext mySqlOffsetContext = loader.load(offset.getOffset());

        if (!isBinlogAvailable(mySqlOffsetContext)) {
            throw new IllegalStateException(
                    "The connector is trying to read binlog starting at "
                            + mySqlOffsetContext.getSourceInfo()
                            + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
        }
        return mySqlOffsetContext;
    }

    private boolean isBinlogAvailable(MySqlOffsetContext offset) {
        String gtidStr = offset.gtidSet();
        if (gtidStr != null) {
            return checkGtidSet(offset);
        }

        return checkBinlogFilename(offset);
    }

    private boolean checkBinlogFilename(MySqlOffsetContext offset) {
        String binlogFilename = offset.getSourceInfo().getString(BINLOG_FILENAME_OFFSET_KEY);
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }

        // Accumulate the available binlog filenames ...
        List<String> logNames = connection.availableBinlogFiles();

        // And compare with the one we're supposed to use ...
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if (!found) {
            LOG.info(
                    "Connector requires binlog file '{}', but MySQL only has {}",
                    binlogFilename,
                    String.join(", ", logNames));
        } else {
            LOG.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }
        return found;
    }

    private boolean checkGtidSet(MySqlOffsetContext offset) {
        String gtidStr = offset.gtidSet();

        if (gtidStr.trim().isEmpty()) {
            return true; // start at beginning ...
        }

        String availableGtidStr = connection.knownGtidSet();
        if (availableGtidStr == null || availableGtidStr.trim().isEmpty()) {
            // Last offsets had GTIDs but the server does not use them ...
            LOG.warn(
                    "Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
            return false;
        }

        // Get the GTID set that is available in the server ...
        GtidSet availableGtidSet = new GtidSet(availableGtidStr);

        // GTIDs are enabled
        LOG.info("Merging server GTID set {} with restored GTID set {}", availableGtidSet, gtidStr);

        // Based on the current server's GTID, the GTID in MySqlOffsetContext is adjusted to ensure
        // the completeness of
        // the GTID. This is done to address the issue of being unable to recover from a checkpoint
        // in certain startup
        // modes.
        GtidSet gtidSet = GtidUtils.fixRestoredGtidSet(availableGtidSet, new GtidSet(gtidStr));
        LOG.info("Merged GTID set is {}", gtidSet);

        if (gtidSet.isContainedWithin(availableGtidSet)) {
            LOG.info(
                    "MySQL current GTID set {} does contain the GTID set {} required by the connector.",
                    availableGtidSet,
                    gtidSet);
            // The replication is concept of mysql master-slave replication protocol ...
            final GtidSet gtidSetToReplicate =
                    connection.subtractGtidSet(availableGtidSet, gtidSet);
            final GtidSet purgedGtidSet = connection.purgedGtidSet();
            LOG.info("Server has already purged {} GTIDs", purgedGtidSet);
            final GtidSet nonPurgedGtidSetToReplicate =
                    connection.subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
            LOG.info(
                    "GTID set {} known by the server but not processed yet, for replication are available only GTID set {}",
                    gtidSetToReplicate,
                    nonPurgedGtidSetToReplicate);
            if (!gtidSetToReplicate.equals(nonPurgedGtidSetToReplicate)) {
                LOG.warn("Some of the GTIDs needed to replicate have been already purged");
                return false;
            }
            return true;
        }
        LOG.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
        return false;
    }

    private void validateAndLoadDatabaseHistory(
            MySqlOffsetContext offset, MySqlDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(Offsets.of(mySqlPartition, offset));
    }

    private void registerDatabaseHistory(SourceSplitBase sourceSplitBase) {
        List<TableChanges.TableChange> engineHistory = new ArrayList<>();
        // TODO: support save table schema
        if (sourceSplitBase instanceof SnapshotSplit) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
            engineHistory.add(
                    dataSourceDialect.queryTableSchema(connection, snapshotSplit.getTableId()));
        } else {
            IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
            Map<TableId, byte[]> historyTableChanges = incrementalSplit.getHistoryTableChanges();
            for (TableId tableId : incrementalSplit.getTableIds()) {
                if (historyTableChanges != null && historyTableChanges.containsKey(tableId)) {
                    SchemaAndValue schemaAndValue =
                            jsonConverter.toConnectData("topic", historyTableChanges.get(tableId));
                    Struct deserializedStruct = (Struct) schemaAndValue.value();

                    TableChanges tableChanges =
                            tableChangeSerializer.deserialize(
                                    Collections.singletonList(deserializedStruct), false);

                    Iterator<TableChanges.TableChange> iterator = tableChanges.iterator();
                    TableChanges.TableChange tableChange = null;
                    while (iterator.hasNext()) {
                        if (tableChange != null) {
                            throw new IllegalStateException(
                                    "The table changes should only have one element");
                        }
                        tableChange = iterator.next();
                    }
                    engineHistory.add(tableChange);
                    continue;
                }
                engineHistory.add(dataSourceDialect.queryTableSchema(connection, tableId));
            }
        }

        EmbeddedDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                engineHistory);
    }

    /** A subclass implementation of {@link MySqlTaskContext} which reuses one BinaryLogClient. */
    public class MySqlTaskContextImpl extends MySqlTaskContext {

        private final BinaryLogClient reusedBinaryLogClient;

        public MySqlTaskContextImpl(
                MySqlConnectorConfig config,
                MySqlDatabaseSchema schema,
                BinaryLogClient reusedBinaryLogClient) {
            super(config, schema);
            this.reusedBinaryLogClient = resetBinaryLogClient(reusedBinaryLogClient);
        }

        @Override
        public BinaryLogClient getBinaryLogClient() {
            return reusedBinaryLogClient;
        }

        /** reset the listener of binaryLogClient before fetch task start. */
        private BinaryLogClient resetBinaryLogClient(BinaryLogClient binaryLogClient) {
            Optional<Object> eventListenersField =
                    ReflectionUtils.getField(
                            binaryLogClient, BinaryLogClient.class, "eventListeners");
            eventListenersField.ifPresent(o -> ((List<BinaryLogClient.EventListener>) o).clear());
            Optional<Object> lifecycleListeners =
                    ReflectionUtils.getField(
                            binaryLogClient, BinaryLogClient.class, "lifecycleListeners");
            lifecycleListeners.ifPresent(
                    o -> ((List<BinaryLogClient.LifecycleListener>) o).clear());
            return binaryLogClient;
        }
    }

    /** Copied from debezium for accessing here. */
    public static class MySqlEventMetadataProvider implements EventMetadataProvider {
        public static final String SERVER_ID_KEY = "server_id";

        public static final String GTID_KEY = "gtid";
        public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
        public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
        public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
        public static final String THREAD_KEY = "thread";
        public static final String QUERY_KEY = "query";

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return Collect.hashMapOf(
                    BINLOG_FILENAME_OFFSET_KEY,
                    sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY),
                    BINLOG_POSITION_OFFSET_KEY,
                    Long.toString(sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY)),
                    BINLOG_ROW_IN_EVENT_OFFSET_KEY,
                    Integer.toString(sourceInfo.getInt32(BINLOG_ROW_IN_EVENT_OFFSET_KEY)));
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return ((MySqlOffsetContext) offset).getTransactionId();
        }
    }
}
