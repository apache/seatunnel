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

package org.apache.seatunnel.e2e.connector.iceberg.recovery;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSource;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSourceFactory;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSplit;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/** Finite, checkpointed CDC script. Gate files release events, never choose a restore offset. */
public class IcebergZetaRecoverySource extends CheckpointableSequenceSource {

    public static final String NAME = "IcebergZetaRecoverySource";
    public static final String FIRST_OFFSET = "observed-first-offset";
    public static final String CDC_GATE = "allow-cdc";
    public static final String RESTORE_GATE = "allow-after-restart";
    public static final String FINISH_GATE = "allow-finish";
    public static final long RESTORE_OFFSET = 9L;

    private final String recoveryRoot;

    public IcebergZetaRecoverySource(ReadonlyConfig config) {
        super(config);
        recoveryRoot = config.get(RecoveryFactory.ROOT);
    }

    @Override
    public String getPluginName() {
        return NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("e2e", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        new PhysicalColumn(
                                                "id",
                                                BasicType.LONG_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        new PhysicalColumn(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        NAME));
    }

    @Override
    public SourceReader<SeaTunnelRow, CheckpointableSequenceSplit> createReader(
            SourceReader.Context context) {
        return new RecoveryReader(context, recoveryRoot);
    }

    public static final class RecoveryFactory implements TableSourceFactory {

        public static final Option<String> ROOT =
                Options.key("recovery_root").stringType().noDefaultValue();

        @Override
        public String factoryIdentifier() {
            return NAME;
        }

        @Override
        public OptionRule optionRule() {
            return OptionRule.builder()
                    .required(ROOT)
                    .optional(
                            CheckpointableSequenceSourceFactory.START_OFFSET,
                            CheckpointableSequenceSourceFactory.END_OFFSET,
                            CheckpointableSequenceSourceFactory.SPLIT_NUM)
                    .build();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T, SplitT extends SourceSplit, StateT extends Serializable>
                TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
            return () ->
                    (SeaTunnelSource<T, SplitT, StateT>)
                            new IcebergZetaRecoverySource(context.getOptions());
        }

        @Override
        public Class<? extends SeaTunnelSource> getSourceClass() {
            return IcebergZetaRecoverySource.class;
        }
    }

    public static final class RecoveryReader
            implements SourceReader<SeaTunnelRow, CheckpointableSequenceSplit> {

        private final Context context;
        private final Path root;
        private final Deque<CheckpointableSequenceSplit> splits = new ConcurrentLinkedDeque<>();
        private volatile boolean noMoreSplits;

        public RecoveryReader(Context context, String root) {
            this.context = context;
            this.root = Paths.get(root);
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
            synchronized (output.getCheckpointLock()) {
                CheckpointableSequenceSplit split = splits.peekFirst();
                if (split == null) {
                    if (noMoreSplits) {
                        context.signalNoMoreElement();
                    }
                } else {
                    // Release the checkpoint lock at each gate so its cursor can be persisted.
                    while (split.hasRemaining() && !blocked(split.getNextOffset())) {
                        output.collect(event(split.getNextOffset()));
                        split.advance();
                    }
                    if (!split.hasRemaining() && Files.isRegularFile(root.resolve(FINISH_GATE))) {
                        splits.removeFirst();
                    }
                }
            }
            Thread.sleep(100L);
        }

        private boolean blocked(long offset) {
            return (offset == 3 && !Files.isRegularFile(root.resolve(CDC_GATE)))
                    || (offset == RESTORE_OFFSET
                            && !Files.isRegularFile(root.resolve(RESTORE_GATE)));
        }

        @Override
        public List<CheckpointableSequenceSplit> snapshotState(long checkpointId) {
            List<CheckpointableSequenceSplit> state = new ArrayList<>();
            for (CheckpointableSequenceSplit split : splits) {
                state.add(
                        new CheckpointableSequenceSplit(
                                split.splitId(),
                                split.getStartOffset(),
                                split.getEndOffset(),
                                split.getNextOffset()));
            }
            return state;
        }

        @Override
        public void addSplits(List<CheckpointableSequenceSplit> restored) {
            if (restored.size() != 1 || !splits.isEmpty()) {
                throw new IllegalStateException("Recovery fixture requires exactly one split");
            }
            try {
                Files.write(
                        root.resolve(FIRST_OFFSET),
                        Long.toString(restored.get(0).getNextOffset())
                                .getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE_NEW);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Cannot record the actual assigned source cursor", e);
            }
            splits.addAll(restored);
        }

        @Override
        public void handleNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        private static SeaTunnelRow event(long offset) {
            RowKind kind;
            long id;
            String value;
            switch ((int) offset) {
                case 0:
                    kind = RowKind.INSERT;
                    id = 1;
                    value = "A0";
                    break;
                case 1:
                    kind = RowKind.INSERT;
                    id = 2;
                    value = "B0";
                    break;
                case 2:
                    kind = RowKind.INSERT;
                    id = 3;
                    value = "C0";
                    break;
                case 3:
                    kind = RowKind.UPDATE_BEFORE;
                    id = 1;
                    value = "A0";
                    break;
                case 4:
                    kind = RowKind.UPDATE_AFTER;
                    id = 1;
                    value = "A1";
                    break;
                case 5:
                    kind = RowKind.DELETE;
                    id = 2;
                    value = "B0";
                    break;
                case 6:
                    kind = RowKind.INSERT;
                    id = 4;
                    value = "D0";
                    break;
                case 7:
                    kind = RowKind.INSERT;
                    id = 90;
                    value = "temporary-before-crash";
                    break;
                case 8:
                    kind = RowKind.DELETE;
                    id = 90;
                    value = "temporary-before-crash";
                    break;
                case 9:
                    kind = RowKind.UPDATE_BEFORE;
                    id = 3;
                    value = "C0";
                    break;
                case 10:
                    kind = RowKind.UPDATE_AFTER;
                    id = 3;
                    value = "C1";
                    break;
                case 11:
                    kind = RowKind.INSERT;
                    id = 5;
                    value = "E0";
                    break;
                case 12:
                    kind = RowKind.DELETE;
                    id = 4;
                    value = "D0";
                    break;
                case 13:
                    kind = RowKind.INSERT;
                    id = 91;
                    value = "temporary-after-restore";
                    break;
                case 14:
                    kind = RowKind.DELETE;
                    id = 91;
                    value = "temporary-after-restore";
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected CDC offset " + offset);
            }
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, value});
            row.setRowKind(kind);
            row.setTableId(TablePath.DEFAULT.toString());
            return row;
        }
    }
}
