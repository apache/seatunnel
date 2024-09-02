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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private HbaseParameters hbaseParameters;

    private List<Integer> rowkeyColumnIndexes = new ArrayList<>();

    private int versionColumnIndex = -1;

    @Override
    public String getPluginName() {
        return HbaseSinkFactory.IDENTIFIER;
    }

    public HbaseSink(HbaseParameters hbaseParameters, CatalogTable catalogTable) {
        this.hbaseParameters = hbaseParameters;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        for (String rowkeyColumn : hbaseParameters.getRowkeyColumns()) {
            this.rowkeyColumnIndexes.add(seaTunnelRowType.indexOf(rowkeyColumn));
        }
        if (hbaseParameters.getVersionColumn() != null) {
            this.versionColumnIndex = seaTunnelRowType.indexOf(hbaseParameters.getVersionColumn());
        }
    }

    @Override
    public HbaseSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new HbaseSinkWriter(
                seaTunnelRowType, hbaseParameters, rowkeyColumnIndexes, versionColumnIndex);
    }
}
