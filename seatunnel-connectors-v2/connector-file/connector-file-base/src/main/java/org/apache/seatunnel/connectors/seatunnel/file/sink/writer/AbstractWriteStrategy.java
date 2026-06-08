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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public abstract class AbstractWriteStrategy<T> implements WriteStrategy<T> {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final FileSinkConfig fileSinkConfig;
    protected final CompressFormat compressFormat;
    // Non-final: rebuilt on each applySchemaChange call. Defensive copy in constructor.
    protected List<Integer> sinkColumnsIndexInRow;
    // Tracks which column names to write. Updated on each schema change event.
    protected List<String> sinkColumnNames;
    // Writer-local partition indices: rebuilt on each applySchemaChange so that ADD COLUMN before
    // a partition column does not leave the partition key reading stale row positions. Defensive
    // copy in constructor — fileSinkConfig is shared across writer instances.
    protected List<Integer> partitionFieldsIndexInRow;
    protected String jobId;
    protected int subTaskIndex;
    protected HadoopConf hadoopConf;
    protected HadoopFileSystemProxy hadoopFileSystemProxy;
    protected String transactionId;
    /** The uuid prefix to make sure same job different file sink will not conflict. */
    protected String uuidPrefix;

    protected String transactionDirectory;
    protected LinkedHashMap<String, String> needMoveFiles;
    protected LinkedHashMap<String, String> beingWrittenFile = new LinkedHashMap<>();
    private LinkedHashMap<String, List<String>> partitionDirAndValuesMap;
    protected SeaTunnelRowType seaTunnelRowType;
    protected TableSchema tableSchema;

    // Checkpoint id from engine is start with 1
    protected Long checkpointId = 0L;
    protected int partId = 0;
    protected int batchSize;
    protected boolean singleFileMode;
    protected int currentBatchSize = 0;

    public AbstractWriteStrategy(FileSinkConfig fileSinkConfig) {
        this.fileSinkConfig = fileSinkConfig;
        // Defensive copies — mutations in applySchemaChange must not affect FileSinkConfig
        // (FileSinkConfig instance is shared across writer instances in multi-writer mode)
        this.sinkColumnsIndexInRow = new ArrayList<>(fileSinkConfig.getSinkColumnsIndexInRow());
        this.sinkColumnNames = new ArrayList<>(fileSinkConfig.getSinkColumnList());
        this.partitionFieldsIndexInRow =
                fileSinkConfig.getPartitionFieldsIndexInRow() == null
                        ? new ArrayList<>()
                        : new ArrayList<>(fileSinkConfig.getPartitionFieldsIndexInRow());
        this.batchSize = fileSinkConfig.getBatchSize();
        this.compressFormat = fileSinkConfig.getCompressFormat();
        this.singleFileMode = fileSinkConfig.isSingleFileMode();
    }

    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    @Override
    public void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex) {
        this.hadoopConf = conf;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(conf);
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.uuidPrefix = uuidPrefix;
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        if (currentBatchSize >= batchSize && !singleFileMode) {
            newFilePart();
            currentBatchSize = 0;
        }
        currentBatchSize++;
    }

    public synchronized void newFilePart() {
        this.partId++;
        beingWrittenFile.clear();
        log.debug("new file part: {}", partId);
    }

    protected SeaTunnelRowType buildSchemaWithRowType(
            SeaTunnelRowType seaTunnelRowType, List<Integer> sinkColumnsIndex) {
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        List<String> newFieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> newFieldTypes = new ArrayList<>();
        sinkColumnsIndex.forEach(
                index -> {
                    newFieldNames.add(fieldNames[index]);
                    newFieldTypes.add(fieldTypes[index]);
                });
        return new SeaTunnelRowType(
                newFieldNames.toArray(new String[0]),
                newFieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param hadoopConf hadoop conf
     * @return Configuration
     */
    @Override
    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = hadoopConf.toConfiguration();
        this.hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    /**
     * set seaTunnelRowTypeInfo in writer
     *
     * @param catalogTable seaTunnelRowType
     */
    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        this.tableSchema = catalogTable.getTableSchema();
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    // ── Schema Evolution ──────────────────────────────────────────────────────────

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        if (!fileSinkConfig.isSchemaEvolutionEnabled()) {
            throw new UnsupportedOperationException(
                    "Received AlterTableEvent but schema_evolution_enabled=false at this sink. "
                            + "Either set schema_evolution_enabled=true to handle schema changes, "
                            + "or set schema-changes.enabled=false at the CDC source to suppress them.");
        }
        log.info(
                "[FileSchemaEvolution] applying {} — before: rowType={}, sinkColumns={}, indices={}",
                event.getClass().getSimpleName(),
                seaTunnelRowType,
                sinkColumnNames,
                sinkColumnsIndexInRow);

        // Step 1: flush + close all open writers; register partial files for commit.
        // Reuses the subclass finishAndCloseFile() — Parquet and ORC each close their own
        // beingWrittenWriter map entries and add to needMoveFiles there.
        // beingWrittenFile.clear() is in a finally block to guarantee it runs even if a writer
        // fails to close, so stale path entries never accumulate in the cache.
        try {
            finishAndCloseFile();
        } finally {
            beingWrittenFile.clear();
        }

        // Step 2: adopt the upstream's actual produced schema, propagated through the chain via
        // event.changeAfter (each transform's mapSchemaChangeEvent updates this field with its own
        // getProducedCatalogTable). This avoids order-divergence — applying ALTER locally would
        // append new cols at the end of the sink's catalog, but upstream's actual row has new
        // cols at the position upstream put them. Reading changeAfter directly aligns the sink's
        // catalog with the actual row layout.
        if (event.getChangeAfter() != null) {
            this.tableSchema = event.getChangeAfter().getTableSchema();
        } else {
            // Fallback for events without changeAfter (older sources or events that bypass the
            // transform chain). Apply ALTER locally as before.
            this.tableSchema = new AlterTableSchemaEventHandler().reset(tableSchema).apply(event);
        }
        this.seaTunnelRowType = tableSchema.toPhysicalRowDataType();

        // Step 3: update sinkColumnNames to reflect the structural change.
        updateSinkColumnNames(event);

        // Step 4: rebuild sinkColumnsIndexInRow from sinkColumnNames + new seaTunnelRowType.
        this.sinkColumnsIndexInRow = rebuildSinkColumnsIndex();

        // Step 4b: rebuild partitionFieldsIndexInRow so partition keys read from correct row
        // positions after ADD/DROP column shifts. Without this, ADD COLUMN before a partition
        // field causes generatorPartitionDir() to read the wrong field for the partition value.
        this.partitionFieldsIndexInRow = rebuildPartitionFieldsIndex();

        // Step 5: let subclasses invalidate any format-specific cached schema objects.
        onSchemaChanged();

        log.info(
                "[FileSchemaEvolution] applied — after: rowType={}, sinkColumns={}, sinkIndices={}, partitionIndices={}",
                seaTunnelRowType,
                sinkColumnNames,
                sinkColumnsIndexInRow,
                partitionFieldsIndexInRow);
    }

    /**
     * Hook for subclasses to invalidate format-specific cached schema objects after a schema
     * change. Default is a no-op. Override in ParquetWriteStrategy to null the cached Avro schema.
     * ORC does not need this because it calls buildSchemaWithRowType() on every write().
     */
    protected void onSchemaChanged() {
        // no-op by default
    }

    /**
     * Bounds-safe row field accessor. Returns null when {@code index >= row.getArity()}.
     *
     * <p>Used in write() loops of all format strategies to handle in-flight rows that were
     * serialised against an old (shorter) schema before a schema change event was processed.
     * Without this guard, row.getField(index) would throw ArrayIndexOutOfBoundsException.
     *
     * @param row the SeaTunnelRow being written
     * @param index the field index from sinkColumnsIndexInRow
     * @return the field value, or null if the row predates the schema change
     */
    protected Object getFieldSafe(SeaTunnelRow row, int index) {
        if (index >= row.getArity()) {
            return null;
        }
        return row.getField(index);
    }

    /**
     * Projects a row to only the sink columns, substituting null for any column whose index exceeds
     * the row's arity. This mirrors {@link SeaTunnelRow#copy(int[])} but is safe for in-flight
     * old-schema rows arriving after an ADD_COLUMN schema change event.
     */
    protected SeaTunnelRow safeProjectedRow(SeaTunnelRow row) {
        int[] indexMapping = sinkColumnsIndexInRow.stream().mapToInt(Integer::intValue).toArray();
        Object[] newFields = new Object[indexMapping.length];
        for (int i = 0; i < indexMapping.length; i++) {
            newFields[i] = indexMapping[i] < row.getArity() ? row.getField(indexMapping[i]) : null;
        }
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(row.getRowKind());
        newRow.setTableId(row.getTableId());
        newRow.setOptions(row.getOptions());
        return newRow;
    }

    private void updateSinkColumnNames(SchemaChangeEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent e = (AlterTableAddColumnEvent) event;
            String newCol = e.getColumn().getName();
            // Case-insensitive duplicate check — CDC sources may send names in any case
            boolean alreadyPresent =
                    sinkColumnNames.stream().anyMatch(c -> c.equalsIgnoreCase(newCol));
            if (!alreadyPresent) {
                if (e.isFirst()) {
                    sinkColumnNames.add(0, newCol);
                } else if (e.getAfterColumn() != null) {
                    int pos = indexOfIgnoreCase(sinkColumnNames, e.getAfterColumn());
                    sinkColumnNames.add(pos >= 0 ? pos + 1 : sinkColumnNames.size(), newCol);
                } else {
                    sinkColumnNames.add(newCol);
                }
            }
        } else if (event instanceof AlterTableDropColumnEvent) {
            // Case-insensitive removal: CDC sources may send column names in any case
            String toDrop = ((AlterTableDropColumnEvent) event).getColumn();
            boolean removed = sinkColumnNames.removeIf(c -> c.equalsIgnoreCase(toDrop));
            if (!removed) {
                log.warn(
                        "[FileSchemaEvolution] DROP event references unknown column '{}' — ignored",
                        toDrop);
            }
        } else if (event instanceof AlterTableChangeColumnEvent) {
            // RENAME (and optional MOVE): remove old name, insert new name at target position.
            // AlterTableChangeColumnEvent covers MySQL's CHANGE COLUMN which can rename and
            // reorder in one statement. Both the rename and the position change are applied.
            AlterTableChangeColumnEvent e = (AlterTableChangeColumnEvent) event;
            int idx = indexOfIgnoreCase(sinkColumnNames, e.getOldColumn());
            if (idx >= 0) {
                sinkColumnNames.remove(idx);
                String newName = e.getColumn().getName();
                if (e.isFirst()) {
                    sinkColumnNames.add(0, newName);
                } else if (e.getAfterColumn() != null) {
                    int afterIdx = indexOfIgnoreCase(sinkColumnNames, e.getAfterColumn());
                    sinkColumnNames.add(
                            afterIdx >= 0 ? afterIdx + 1 : sinkColumnNames.size(), newName);
                } else {
                    // No position change — restore at the same index
                    sinkColumnNames.add(idx, newName);
                }
            }
        } else if (event instanceof AlterTableModifyColumnEvent) {
            // TYPE change only — column name is unchanged, no list update needed
        } else if (event instanceof AlterTableColumnsEvent) {
            // Batch: apply each sub-event in order
            for (AlterTableColumnEvent sub : ((AlterTableColumnsEvent) event).getEvents()) {
                updateSinkColumnNames(sub);
            }
        }
    }

    /** Case-insensitive indexOf for a list of column names. Returns -1 if not found. */
    private static int indexOfIgnoreCase(List<String> list, String name) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    private List<Integer> rebuildSinkColumnsIndex() {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Map<String, Integer> nameToIndex = new HashMap<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            nameToIndex.put(fieldNames[i].toLowerCase(), i);
        }
        List<Integer> newIndex = new ArrayList<>(sinkColumnNames.size());
        for (String col : sinkColumnNames) {
            Integer idx = nameToIndex.get(col.toLowerCase());
            if (idx != null) {
                newIndex.add(idx);
            }
            // Column not found in new rowType (e.g. just dropped) — skip silently
        }
        return newIndex;
    }

    /**
     * Rebuilds partition-field row indices from the configured partition column names against the
     * post-ALTER {@link #seaTunnelRowType}. Partition column NAMES are immutable (set once at job
     * start); only their row positions can shift when other columns are added/dropped.
     *
     * <p>Throws if a configured partition column has been dropped from the schema — a dropped
     * partition key would corrupt the partition tree, so we fail fast rather than silently writing
     * to the wrong directory.
     */
    private List<Integer> rebuildPartitionFieldsIndex() {
        List<String> partitionFieldList = fileSinkConfig.getPartitionFieldList();
        if (partitionFieldList == null || partitionFieldList.isEmpty()) {
            return new ArrayList<>();
        }
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Map<String, Integer> nameToIndex = new HashMap<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            nameToIndex.put(fieldNames[i].toLowerCase(), i);
        }
        List<Integer> newIndex = new ArrayList<>(partitionFieldList.size());
        for (String col : partitionFieldList) {
            Integer idx = nameToIndex.get(col.toLowerCase());
            if (idx == null) {
                throw new IllegalStateException(
                        "Schema evolution: partition column ["
                                + col
                                + "] is missing from the post-ALTER schema. "
                                + "Dropping or renaming a partition column is not supported. "
                                + "Available columns: "
                                + java.util.Arrays.toString(fieldNames));
            }
            newIndex.add(idx);
        }
        return newIndex;
    }

    /**
     * use seaTunnelRow generate partition directory
     *
     * @param seaTunnelRow seaTunnelRow
     * @return the map of partition directory
     */
    @Override
    public LinkedHashMap<String, List<String>> generatorPartitionDir(SeaTunnelRow seaTunnelRow) {
        // Use writer-local indices (not fileSinkConfig.getPartitionFieldsIndexInRow()): these get
        // rebuilt in applySchemaChange so they stay aligned with the post-ALTER row layout.
        List<Integer> partitionFieldsIndexInRow = this.partitionFieldsIndexInRow;
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>(1);
        if (CollectionUtils.isEmpty(partitionFieldsIndexInRow)) {
            partitionDirAndValuesMap.put(FileBaseSinkOptions.NON_PARTITION, null);
            return partitionDirAndValuesMap;
        }
        List<String> partitionFieldList = fileSinkConfig.getPartitionFieldList();
        String partitionDirExpression = fileSinkConfig.getPartitionDirExpression();
        String[] keys = new String[partitionFieldList.size()];
        String[] values = new String[partitionFieldList.size()];
        for (int i = 0; i < partitionFieldList.size(); i++) {
            keys[i] = "k" + i;
            values[i] = "v" + i;
        }
        List<String> vals = new ArrayList<>(partitionFieldsIndexInRow.size());
        String partitionDir;
        if (StringUtils.isBlank(partitionDirExpression)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                stringBuilder
                        .append(partitionFieldList.get(i))
                        .append("=")
                        .append(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)]);
                if (i < partitionFieldsIndexInRow.size() - 1) {
                    stringBuilder.append("/");
                }
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = stringBuilder.toString();
        } else {
            Map<String, String> valueMap = new HashMap<>(partitionFieldList.size() * 2);
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                valueMap.put(keys[i], partitionFieldList.get(i));
                valueMap.put(
                        values[i],
                        seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = VariablesSubstitute.substitute(partitionDirExpression, valueMap);
        }
        partitionDirAndValuesMap.put(partitionDir, vals);
        return partitionDirAndValuesMap;
    }

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    @Override
    public final String generateFileName(String transactionId) {
        String fileNameExpression = fileSinkConfig.getFileNameExpression();
        FileFormat fileFormat = fileSinkConfig.getFileFormat();
        String suffix;
        if (StringUtils.isNotEmpty(fileSinkConfig.getFilenameExtension())) {
            suffix =
                    fileSinkConfig.getFilenameExtension().startsWith(".")
                            ? fileSinkConfig.getFilenameExtension()
                            : "." + fileSinkConfig.getFilenameExtension();
        } else {
            suffix = fileFormat.getSuffix();
            suffix = compressFormat.getCompressCodec() + suffix;
        }
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + suffix;
        }
        String timeFormat = fileSinkConfig.getFileNameTimeFormat();
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        String formattedDate = df.format(ZonedDateTime.now());
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        valuesMap.put(FileBaseSinkOptions.TRANSACTION_EXPRESSION, transactionId);
        String substitute = VariablesSubstitute.substitute(fileNameExpression, valuesMap);
        if (!singleFileMode) {
            substitute += "_" + partId;
        }
        return substitute + suffix;
    }

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    @SneakyThrows
    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        if (this.needMoveFiles.isEmpty() && fileSinkConfig.isCreateEmptyFileWhenNoData()) {
            String filePath = createFilePathWithoutPartition();
            this.getOrCreateOutputStream(filePath);
        }
        this.finishAndCloseFile();
        LinkedHashMap<String, String> commitMap = new LinkedHashMap<>(this.needMoveFiles);
        LinkedHashMap<String, List<String>> copyMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));
        return Optional.of(new FileCommitInfo(commitMap, copyMap, transactionDirectory));
    }

    /** abort prepare commit operation */
    @Override
    public void abortPrepare() {
        abortPrepare(transactionId);
    }

    /**
     * abort prepare commit operation using transaction directory
     *
     * @param transactionId transaction id
     */
    public void abortPrepare(String transactionId) {
        String transactionDir = getTransactionDir(transactionId);
        try {
            hadoopFileSystemProxy.deleteFile(transactionDir);
            cleanupTransactionParentDirectories(transactionDir);
        } catch (IOException e) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Abort transaction "
                            + transactionId
                            + " error, delete transaction directory failed",
                    e);
        }
    }

    private void cleanupTransactionParentDirectories(String transactionDir) {
        Path uuidDir = new Path(transactionDir).getParent();
        if (uuidDir == null) {
            return;
        }
        Path jobDir = uuidDir.getParent();
        if (jobDir == null) {
            return;
        }
        cleanupEmptyDirectory(uuidDir);
        cleanupEmptyDirectory(jobDir);
    }

    private void cleanupEmptyDirectory(Path directory) {
        try {
            hadoopFileSystemProxy.deleteEmptyDirectory(directory.toString());
        } catch (IOException e) {
            log.warn("Failed to clean empty transaction parent directory: {}", directory, e);
        }
    }

    /**
     * when a checkpoint completed, file connector should begin a new transaction and generate new
     * transaction id
     *
     * @param checkpointId checkpoint id
     */
    public void beginTransaction(Long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = getTransactionId(checkpointId);
        this.transactionDirectory = getTransactionDir(this.transactionId);
        this.needMoveFiles = new LinkedHashMap<>();
        this.partitionDirAndValuesMap = new LinkedHashMap<>();
    }

    private String getTransactionId(Long checkpointId) {
        return "T"
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + jobId
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + uuidPrefix
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + subTaskIndex
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + checkpointId;
    }

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     *
     * @param checkpointId checkpointId
     * @return the list of states
     */
    @Override
    public List<FileSinkState> snapshotState(long checkpointId) {
        LinkedHashMap<String, List<String>> commitMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));
        ArrayList<FileSinkState> fileState =
                Lists.newArrayList(
                        new FileSinkState(
                                this.transactionId,
                                this.uuidPrefix,
                                this.checkpointId,
                                new LinkedHashMap<>(this.needMoveFiles),
                                commitMap,
                                this.getTransactionDir(transactionId)));
        this.beingWrittenFile.clear();
        this.beginTransaction(checkpointId + 1);
        return fileState;
    }

    /**
     * using transaction id generate transaction directory
     *
     * @param transactionId transaction id
     * @return transaction directory
     */
    private String getTransactionDir(@NonNull String transactionId) {
        String transactionDirectoryPrefix =
                getTransactionDirPrefix(fileSinkConfig.getTmpPath(), jobId, uuidPrefix);
        return String.join(
                File.separator, new String[] {transactionDirectoryPrefix, transactionId});
    }

    public static String getTransactionDirPrefix(String tmpPath, String jobId, String uuidPrefix) {
        String[] strings = new String[] {tmpPath, FileBaseSinkOptions.SEATUNNEL, jobId, uuidPrefix};
        return String.join(File.separator, strings);
    }

    public String createFilePathWithoutPartition() {
        return getPathWithPartitionInfo(null, true);
    }

    public String getOrCreateFilePathBeingWritten(@NonNull SeaTunnelRow seaTunnelRow) {
        LinkedHashMap<String, List<String>> dataPartitionDirAndValuesMap =
                generatorPartitionDir(seaTunnelRow);
        boolean noPartition =
                FileBaseSinkOptions.NON_PARTITION.equals(
                        dataPartitionDirAndValuesMap.keySet().toArray()[0].toString());
        return getPathWithPartitionInfo(dataPartitionDirAndValuesMap, noPartition);
    }

    private String getPathWithPartitionInfo(
            LinkedHashMap<String, List<String>> dataPartitionDirAndValuesMap, boolean noPartition) {
        String beingWrittenFileKey =
                noPartition
                        ? FileBaseSinkOptions.NON_PARTITION
                        : dataPartitionDirAndValuesMap.keySet().toArray()[0].toString();
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] pathSegments =
                    new String[] {
                        transactionDirectory, beingWrittenFileKey, generateFileName(transactionId)
                    };
            String newBeingWrittenFilePath = String.join(File.separator, pathSegments);
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            if (!noPartition) {
                partitionDirAndValuesMap.putAll(dataPartitionDirAndValuesMap);
            }
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String seaTunnelFilePath) {
        String tmpPath =
                seaTunnelFilePath.replaceAll(
                        Matcher.quoteReplacement(transactionDirectory),
                        Matcher.quoteReplacement(fileSinkConfig.getPath()));
        return tmpPath.replaceAll(
                FileBaseSinkOptions.NON_PARTITION + Matcher.quoteReplacement(File.separator), "");
    }

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public FileSinkConfig getFileSinkConfig() {
        return fileSinkConfig;
    }

    @Override
    public HadoopFileSystemProxy getHadoopFileSystemProxy() {
        return hadoopFileSystemProxy;
    }

    @Override
    public void close() throws IOException {
        try {
            if (hadoopFileSystemProxy != null) {
                hadoopFileSystemProxy.close();
            }
        } catch (Exception ignore) {
        }
    }
}
