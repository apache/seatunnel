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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.util.Collections;
import java.util.List;

/** Bounded error-event extraction for one project, without remote snapshot guarantees. */
public class SentrySource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final SentrySourceConfig config;

    public SentrySource(ReadonlyConfig options) {
        config = new SentrySourceConfig(options);
    }

    @Override
    public String getPluginName() {
        return "Sentry";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw SentrySourceConfig.failure("Sentry source supports BATCH only");
        }
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema.Builder schema = TableSchema.builder();
        String[] fields = {
            "event_id",
            "group_id",
            "project_id",
            "date_created",
            "title",
            "message",
            "platform",
            "content"
        };
        for (String name : fields) {
            schema.column(
                    PhysicalColumn.of(
                            name,
                            BasicType.STRING_TYPE,
                            0,
                            !name.equals("event_id") && !name.equals("content"),
                            null,
                            null));
        }
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("Sentry", TablePath.DEFAULT),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new SentrySourceReader(config, context);
    }
}
