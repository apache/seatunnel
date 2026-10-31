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

package org.apache.seatunnel.connectors.seatunnel.syslog.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogConfig;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSourceOptions;

import java.util.Collections;
import java.util.List;

public class SyslogSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {
                        "facility",
                        "severity",
                        "timestamp",
                        "hostname",
                        "app_name",
                        "proc_id",
                        "message"
                    },
                    new SeaTunnelDataType<?>[] {
                        BasicType.INT_TYPE,
                        BasicType.INT_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE
                    });

    private final SyslogConfig config;
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public SyslogSource(ReadonlyConfig pluginConfig) {
        this.config = new SyslogConfig(pluginConfig);
        this.catalogTable =
                CatalogTableUtil.getCatalogTable(SyslogSourceOptions.IDENTIFIER, ROW_TYPE);
    }

    @Override
    public String getPluginName() {
        return SyslogSourceOptions.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
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
            SingleSplitReaderContext readerContext) throws Exception {
        return new SyslogSourceReader(config, readerContext);
    }
}
