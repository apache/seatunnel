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

package org.apache.seatunnel.connectors.seatunnel.snmp.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceOptions;

import java.util.Collections;
import java.util.List;

public class SnmpSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    static final String FIELD_AGENT = "agent";
    static final String FIELD_OID = "oid";
    static final String FIELD_VALUE = "value";
    static final String FIELD_VALUE_TYPE = "value_type";
    static final String FIELD_POLL_TIME = "poll_time";

    private final SnmpSourceConfig config;
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public SnmpSource(SnmpSourceConfig config) {
        this.config = config;
        this.catalogTable = createCatalogTable();
    }

    @Override
    public String getPluginName() {
        return SnmpSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return jobContext != null && JobMode.STREAMING.equals(jobContext.getJobMode())
                ? Boundedness.UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new SnmpSourceReader(config, readerContext);
    }

    static CatalogTable createCatalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        FIELD_AGENT, BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        FIELD_OID, BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        FIELD_VALUE, BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        FIELD_VALUE_TYPE,
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        FIELD_POLL_TIME, BasicType.LONG_TYPE, 0, false, null, null))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "snmp"),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "SNMP source output");
    }
}
