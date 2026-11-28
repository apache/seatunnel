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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

/** Reads the Adjust JSON report as schema-derived daily slices. */
public final class AdjustSource
        implements SeaTunnelSource<SeaTunnelRow, AdjustScan, AdjustEnumerator.State> {
    static final String ID = "Adjust";
    private static final long serialVersionUID = 1L;
    private final AdjustReportPlan plan;
    private final CatalogTable table;

    AdjustSource(AdjustReportPlan plan) {
        this.plan = plan;
        table =
                CatalogTable.of(
                        TableIdentifier.of(ID, TablePath.of("adjust_report")),
                        plan.schema(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Daily Adjust report");
    }

    @Override
    public String getPluginName() {
        return ID;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, AdjustScan> createReader(SourceReader.Context context) {
        return new AdjustReader(context, plan, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<AdjustScan, AdjustEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<AdjustScan> context) {
        return new AdjustEnumerator(context, plan, null);
    }

    @Override
    public SourceSplitEnumerator<AdjustScan, AdjustEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<AdjustScan> context, AdjustEnumerator.State state) {
        return new AdjustEnumerator(context, plan, state);
    }
}
