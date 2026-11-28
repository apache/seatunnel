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

package org.apache.seatunnel.connectors.seatunnel.feishu.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;

import java.util.Collections;
import java.util.List;

/**
 * Defines one bounded Bitable table scan and its fixed four-String output schema. Split assignment
 * gates reader I/O, while the existing webhook Sink remains independent in the same artifact.
 */
public class FeishuSource
        implements SeaTunnelSource<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState> {

    private static final long serialVersionUID = 1L;
    static final String PLUGIN_NAME = "Feishu";

    private final FeishuSourceOptions config;
    private final CatalogTable catalogTable;
    // Engine configuration is used to reject streaming execution before readers perform I/O.
    private JobContext jobContext;

    /**
     * Captures validated source settings and the fixed catalog once. Dynamic Bitable fields stay
     * inside JSON columns rather than changing the schema during a paginated scan.
     */
    FeishuSource(ReadonlyConfig options) {
        config = new FeishuSourceOptions(options);
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "record_id",
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        "Bitable record ID"))
                        .column(
                                PhysicalColumn.of(
                                        "id",
                                        BasicType.STRING_TYPE,
                                        0,
                                        true,
                                        null,
                                        "Optional provider record ID"))
                        .column(
                                PhysicalColumn.of(
                                        "fields",
                                        BasicType.STRING_TYPE,
                                        0,
                                        true,
                                        null,
                                        "Bitable field values as JSON"))
                        .column(
                                PhysicalColumn.of(
                                        "record_json",
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        "Complete Bitable record as JSON"))
                        .build();
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(PLUGIN_NAME, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    /**
     * Restricts this full-refresh implementation to batch execution; provider cursors are scan
     * positions and do not provide a change-event stream or transactional snapshot.
     */
    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null && jobContext.getJobMode() != JobMode.BATCH) {
            throw new UnsupportedOperationException("Feishu source only supports batch jobs");
        }
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    /**
     * Creates the sole scan owner. Rejecting additional reader subtasks prevents independent
     * readers from duplicating the same table refresh.
     */
    @Override
    public SourceReader<SeaTunnelRow, SingleSplit> createReader(SourceReader.Context context) {
        if (context.getIndexOfSubtask() != 0) {
            throw new IllegalArgumentException("Feishu source requires parallelism = 1");
        }
        return new FeishuSourceReader(config, context);
    }

    @Override
    public SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> context) {
        return new FeishuSplitEnumerator(context);
    }

    /**
     * Recreates assignment ownership while the engine restores row progress through reader splits.
     * An early empty checkpoint must retain the normal first assignment instead of ending the scan.
     */
    @Override
    public SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> context, SingleSplitEnumeratorState state) {
        return createEnumerator(context);
    }

    /**
     * Serializes the existing single-split assignment hooks and preserves initial assignment for an
     * early empty reader checkpoint. Nonempty restored progress follows the shared split path.
     */
    private static final class FeishuSplitEnumerator extends SingleSplitEnumerator {
        private FeishuSplitEnumerator(Context<SingleSplit> context) {
            super(context);
        }

        @Override
        public synchronized void run() throws Exception {
            super.run();
        }

        @Override
        public synchronized void registerReader(int subtaskId) {
            super.registerReader(subtaskId);
        }

        @Override
        public synchronized void addSplitsBack(List<SingleSplit> splits, int subtaskId) {
            // An early checkpoint can record a registered reader before its first assignment.
            // Retain the normal first assignment in run() when there is no progress to restore.
            if (!splits.isEmpty()) {
                super.addSplitsBack(splits, subtaskId);
            }
        }
    }
}
