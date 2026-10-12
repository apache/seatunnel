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

package org.apache.seatunnel.connectors.seatunnel.file.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

public abstract class BaseMultipleTableFileSink
        implements SeaTunnelSink<
                        SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>,
                SupportMultiTableSink,
                SupportSaveMode,
                SupportSchemaEvolutionSink {

    private static final long serialVersionUID = 1L;

    private final HadoopConf hadoopConf;
    private final CatalogTable catalogTable;
    private final FileSinkConfig fileSinkConfig;
    private String jobId;
    private JobContext jobContext;
    private final ReadonlyConfig readonlyConfig;

    public abstract String getPluginName();

    public BaseMultipleTableFileSink(
            HadoopConf hadoopConf, ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.hadoopConf = hadoopConf;
        this.fileSinkConfig =
                new FileSinkConfig(readonlyConfig, catalogTable.getSeaTunnelRowType());
        this.catalogTable = catalogTable;
    }

    public void preCheckConfig() {
        if (readonlyConfig.get(FileBaseSinkOptions.SINGLE_FILE_MODE)
                && jobContext.isEnableCheckpoint()) {
            throw new IllegalArgumentException(
                    "Single file mode is not supported when checkpoint is enabled or in streaming mode.");
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
        preCheckConfig();
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new BaseFileSinkWriter(
                createWriteStrategy(), hadoopConf, context, jobContext.getJobId(), states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(new FileSinkAggregatedCommitter(hadoopConf));
    }

    @Override
    public BaseFileSinkWriter createWriter(SinkWriter.Context context) {
        return new BaseFileSinkWriter(
                createWriteStrategy(), hadoopConf, context, jobContext.getJobId());
    }

    @Override
    public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    /**
     * Shares a writer only for aliases with the same static row layout and the same physical file
     * destination. Schema-evolution jobs keep isolated writers because each source alias can evolve
     * its own in-memory row layout.
     *
     * <p>The identifier folds in every coordinate that distinguishes a file destination: filesystem
     * endpoint (via {@code hadoopConf}), path, file format, tmp path and file-name expression, plus
     * the static row type. Connection-level settings from {@code hadoopConf} are folded in as a
     * stable hash rather than verbatim, so divergent buckets, endpoints or credentials produce
     * distinct identifiers without embedding secrets in the identifier string. Two aliases that
     * differ in any of these coordinates never share a writer.
     *
     * @return a physical file destination identifier when writer sharing is safe, otherwise empty
     */
    @Override
    public Optional<String> getPhysicalDestinationIdentifier() {
        if (fileSinkConfig.isSchemaEvolutionEnabled()) {
            return Optional.empty();
        }
        String path = fileSinkConfig.getPath();
        if (path == null) {
            return Optional.empty();
        }
        String rowType = String.valueOf(catalogTable.getSeaTunnelRowType());
        String fsName = hadoopConf != null ? String.valueOf(hadoopConf.getHdfsNameKey()) : "";
        String format = String.valueOf(fileSinkConfig.getFileFormat());
        String tmpPath = String.valueOf(fileSinkConfig.getTmpPath());
        String fileNameExpression = String.valueOf(fileSinkConfig.getFileNameExpression());
        String fsOptionsHash = hashHadoopOptions();
        return Optional.of(
                String.join(
                        ";",
                        "path=" + path,
                        "fs=" + fsName,
                        "format=" + format,
                        "tmp=" + tmpPath,
                        "fileName=" + fileNameExpression,
                        "row-type=" + rowType,
                        "fsOpts=" + fsOptionsHash));
    }

    /**
     * Hashes filesystem connection options into a stable, opaque token.
     *
     * <p>Options may carry endpoints, buckets and credential-adjacent settings. They must
     * distinguish identifiers, but must not be embedded verbatim.
     *
     * @return hex hash of the sorted option entries, or {@code empty} when none are set
     */
    private String hashHadoopOptions() {
        if (hadoopConf == null
                || hadoopConf.getExtraOptions() == null
                || hadoopConf.getExtraOptions().isEmpty()) {
            return "empty";
        }
        Map<String, String> sorted = new TreeMap<>(hadoopConf.getExtraOptions());
        return Integer.toHexString(sorted.toString().hashCode());
    }

    @Override
    public List<SchemaChangeType> supports() {
        if (!fileSinkConfig.isSchemaEvolutionEnabled()) {
            return Collections.emptyList();
        }
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }

    protected WriteStrategy createWriteStrategy() {
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setCatalogTable(catalogTable);
        return writeStrategy;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {

        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        getPluginName());
        if (catalogFactory == null) {
            return Optional.empty();
        }
        final Catalog catalog = catalogFactory.createCatalog(getPluginName(), readonlyConfig);
        SchemaSaveMode schemaSaveMode = readonlyConfig.get(FileBaseSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = readonlyConfig.get(FileBaseSinkOptions.DATA_SAVE_MODE);
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, catalogTable, null));
    }
}
