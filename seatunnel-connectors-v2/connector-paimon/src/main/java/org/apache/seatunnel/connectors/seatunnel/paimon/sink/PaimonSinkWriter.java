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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssigner;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssignerFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.RowAssignerChannelComputer;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.schema.handler.AlterPaimonTableSchemaEventHandler;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.table.sink.WriteBuilder;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

@Slf4j
public class PaimonSinkWriter
        implements SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {

    private String commitUser = UUID.randomUUID().toString();

    private FileStoreTable paimonFileStoretable;

    private WriteBuilder tableWriteBuilder;

    private TableWrite tableWrite;

    private final List<CommitMessage> committables = new ArrayList<>();

    private SeaTunnelRowType seaTunnelRowType;

    private org.apache.seatunnel.api.table.catalog.TableSchema sourceTableSchema;

    private TableSchema sinkPaimonTableSchema;

    private final boolean dynamicBucket;

    private final PaimonBucketAssignerFactory paimonBucketAssignerFactory;

    private final PaimonCatalog paimonCatalog;

    private final TablePath paimonTablePath;

    private final PaimonSinkConfig paimonSinkConfig;

    private final TableSchemaChangeEventDispatcher TABLE_SCHEMACHANGER =
            new TableSchemaChangeEventDispatcher();

    private final JobContext jobContext;

    private final RowAssignerChannelComputer rowAssignerChannelComputer;

    private final int parallelism;

    private final int taskIndex;

    private final Set<PaimonBucketAssigner> bucketAssigners = new HashSet<>();

    public PaimonSinkWriter(
            Context context,
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            Table paimonTable,
            JobContext jobContext,
            PaimonSinkConfig paimonSinkConfig,
            PaimonHadoopConfiguration paimonHadoopConfiguration,
            PaimonBucketAssignerFactory paimonBucketAssignerFactory) {
        this.sourceTableSchema = catalogTable.getTableSchema();
        this.seaTunnelRowType = this.sourceTableSchema.toPhysicalRowDataType();
        this.jobContext = jobContext;
        this.paimonTablePath = catalogTable.getTablePath();
        this.paimonCatalog = PaimonCatalog.loadPaimonCatalog(readonlyConfig);
        this.paimonCatalog.open();
        this.paimonFileStoretable = (FileStoreTable) paimonTable;
        CoreOptions.ChangelogProducer changelogProducer =
                this.paimonFileStoretable.coreOptions().changelogProducer();
        if (Objects.nonNull(paimonSinkConfig.getChangelogProducer())
                && changelogProducer != paimonSinkConfig.getChangelogProducer()) {
            log.warn(
                    "configured the props named 'changelog-producer' which is not compatible with the options in table , so it will use the table's 'changelog-producer'");
        }
        this.rowAssignerChannelComputer =
                new RowAssignerChannelComputer(
                        paimonFileStoretable.schema(), context.getNumberOfParallelSubtasks());
        rowAssignerChannelComputer.setup(context.getNumberOfParallelSubtasks());
        this.paimonBucketAssignerFactory = paimonBucketAssignerFactory;
        this.parallelism = context.getNumberOfParallelSubtasks();
        this.taskIndex = context.getIndexOfSubtask();
        this.paimonSinkConfig = paimonSinkConfig;
        this.sinkPaimonTableSchema = this.paimonFileStoretable.schema();
        this.newTableWrite();
        BucketMode bucketMode = this.paimonFileStoretable.bucketMode();
        // https://paimon.apache.org/docs/master/primary-key-table/data-distribution/#dynamic-bucket
        // When you need cross partition upsert (primary keys not contain all partition fields),
        // Dynamic Bucket mode directly maintains the mapping of keys to partition and bucket, uses
        // local disks, and initializes indexes by reading all existing keys in the table when
        // starting job. For tables with a large amount of data, there will be a significant loss in
        // performance. Moreover, initialization takes a long time. This mode is not supported at
        // this time.
        if (BucketMode.GLOBAL_DYNAMIC == bucketMode) {
            throw new UnsupportedOperationException(
                    "Cross Partitions Upsert Dynamic Bucket Mode is not supported.");
        }
        this.dynamicBucket = BucketMode.DYNAMIC == bucketMode;
        int bucket = ((FileStoreTable) paimonTable).coreOptions().bucket();
        if (bucket == -1 && BucketMode.UNAWARE == bucketMode) {
            log.warn("Append only table currently do not support dynamic bucket");
        }
        if (dynamicBucket) {
            paimonBucketAssignerFactory.init(paimonTablePath, paimonFileStoretable, parallelism);
        }
        PaimonSecurityContext.shouldEnableKerberos(paimonHadoopConfiguration);
    }

    public PaimonSinkWriter(
            Context context,
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            Table paimonFileStoretable,
            List<PaimonSinkState> states,
            JobContext jobContext,
            PaimonSinkConfig paimonSinkConfig,
            PaimonHadoopConfiguration paimonHadoopConfiguration,
            PaimonBucketAssignerFactory paimonBucketAssignerFactory) {
        this(
                context,
                readonlyConfig,
                catalogTable,
                paimonFileStoretable,
                jobContext,
                paimonSinkConfig,
                paimonHadoopConfiguration,
                paimonBucketAssignerFactory);
        if (Objects.isNull(states) || states.isEmpty()) {
            return;
        }
        this.commitUser = states.get(0).getCommitUser();
        long checkpointId = states.get(0).getCheckpointId();
        try (TableCommit tableCommit = tableWriteBuilder.newCommit()) {
            List<CommitMessage> commitables =
                    states.stream()
                            .map(PaimonSinkState::getCommittables)
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
            // batch mode without checkpoint has no state to commit
            if (commitables.isEmpty()) {
                return;
            }
            // streaming mode or batch mode with checkpoint need to recommit by stream api
            log.info("Trying to recommit states {}", commitables);
            ((StreamTableCommit) tableCommit).commit(checkpointId, commitables);
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED, e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        InternalRow rowData =
                RowConverter.reconvert(element, seaTunnelRowType, sinkPaimonTableSchema);
        try {
            PaimonSecurityContext.runSecured(
                    () -> {
                        if (dynamicBucket) {
                            // The result of calculating the remainder of the parallelism using the
                            // hash code of the primary key must be consistent with the task
                            // sequence number.
                            PaimonBucketAssigner bucketAssigner =
                                    paimonBucketAssignerFactory.getBucketAssigner(
                                            paimonTablePath,
                                            rowAssignerChannelComputer.channel(rowData));
                            // When multiple threads call assigner.assign() simultaneously, they can
                            // corrupt the internal hash map structure, leading to the
                            // ArrayIndexOutOfBoundsException during rehashing operations
                            synchronized (bucketAssigner) {
                                tableWrite.write(rowData, bucketAssigner.assign(rowData));
                                bucketAssigners.add(bucketAssigner);
                            }
                        } else {
                            tableWrite.write(rowData);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_RECORD_FAILED,
                    "This record " + element + " failed to be written",
                    e);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        this.sourceTableSchema =
                new AlterPaimonTableSchemaEventHandler(
                                sourceTableSchema,
                                paimonCatalog,
                                sinkPaimonTableSchema,
                                paimonTablePath)
                        .apply(event);
        reOpenTableWrite();
    }

    private void reOpenTableWrite() {
        this.seaTunnelRowType = this.sourceTableSchema.toPhysicalRowDataType();
        this.paimonFileStoretable = (FileStoreTable) paimonCatalog.getPaimonTable(paimonTablePath);
        this.sinkPaimonTableSchema = this.paimonFileStoretable.schema();
        this.newTableWrite();
    }

    private void newTableWrite() {
        this.tableWriteBuilder = this.paimonFileStoretable.newStreamWriteBuilder();
        TableWrite oldTableWrite = this.tableWrite;
        this.tableWrite =
                tableWriteBuilder
                        .newWrite()
                        .withIOManager(
                                IOManager.create(
                                        splitPaths(paimonSinkConfig.getChangelogTmpPath())));
        tableWriteClose(oldTableWrite);
    }

    @Override
    public Optional<PaimonCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<PaimonCommitInfo> prepareCommit(long checkpointId) throws IOException {
        try {
            List<CommitMessage> fileCommittables =
                    ((StreamTableWrite) tableWrite).prepareCommit(waitCompaction(), checkpointId);
            committables.addAll(fileCommittables);
            if (!bucketAssigners.isEmpty()) {
                List<PaimonBucketAssigner> assigners = new ArrayList<>(bucketAssigners);
                bucketAssigners.clear();
                assigners.forEach(assigner -> assigner.prepareCommit(checkpointId));
            }
            return Optional.of(new PaimonCommitInfo(fileCommittables, checkpointId));
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_PRE_COMMIT_FAILED,
                    "Paimon pre-commit failed.",
                    e);
        }
    }

    @Override
    public List<PaimonSinkState> snapshotState(long checkpointId) throws IOException {
        PaimonSinkState paimonSinkState =
                new PaimonSinkState(new ArrayList<>(committables), commitUser, checkpointId);
        committables.clear();
        return Collections.singletonList(paimonSinkState);
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        try {
            tableWriteClose(this.tableWrite);
        } finally {
            committables.clear();
            paimonBucketAssignerFactory.clear(paimonTablePath, taskIndex);
            if (Objects.nonNull(paimonCatalog)) {
                paimonCatalog.close();
            }
        }
    }

    private void tableWriteClose(TableWrite tableWrite) {
        if (Objects.nonNull(tableWrite)) {
            try {
                tableWrite.close();
            } catch (Exception e) {
                log.error("Failed to close table writer in paimon sink writer.", e);
                throw new SeaTunnelException(e);
            }
        }
    }

    @VisibleForTesting
    public boolean waitCompaction() {
        if (JobMode.BATCH.equals(jobContext.getJobMode())) {
            return true;
        }
        CoreOptions coreOptions = this.paimonFileStoretable.coreOptions();
        if (coreOptions.writeOnly()) {
            return false;
        }
        CoreOptions.ChangelogProducer changelogProducer = coreOptions.changelogProducer();
        return changelogProducer == CoreOptions.ChangelogProducer.LOOKUP
                || changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION;
    }
}
