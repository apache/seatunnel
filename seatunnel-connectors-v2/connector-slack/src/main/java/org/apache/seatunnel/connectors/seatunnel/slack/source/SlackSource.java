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

package org.apache.seatunnel.connectors.seatunnel.slack.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.util.Collections;
import java.util.List;

/**
 * Provides the fixed Slack history schema and one bounded reader. Source parallelism is one
 * regardless of the engine job mode.
 */
public class SlackSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final SlackSourceConfig config;

    /**
     * Retains the validated frozen window shared by this single-reader source's serialized copies.
     */
    public SlackSource(SlackSourceConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "Slack";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema.Builder schema = TableSchema.builder();
        for (String field :
                new String[] {
                    "channel_id", "ts", "thread_ts", "user", "text", "type", "subtype", "raw_json"
                }) {
            schema.column(
                    PhysicalColumn.of(
                            field,
                            BasicType.STRING_TYPE,
                            0,
                            !field.equals("channel_id")
                                    && !field.equals("ts")
                                    && !field.equals("raw_json"),
                            null,
                            null));
        }
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "slack_history"),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Bounded Slack conversation history"));
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new SlackSourceReader(config, context);
    }
}
