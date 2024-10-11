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

package org.apache.seatunnel.connectors.seatunnel.kudu.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSinkState;

import java.io.IOException;

/**
 * Kudu Sink implementation by using SeaTunnel sink API. This class contains the method to create
 * {@link AbstractSimpleSink}.
 */
public class KuduSink
        implements SeaTunnelSink<
                        SeaTunnelRow, KuduSinkState, KuduCommitInfo, KuduAggregatedCommitInfo>,
                SupportMultiTableSink {

    private KuduSinkConfig kuduSinkConfig;
    private SeaTunnelRowType seaTunnelRowType;

    public KuduSink(KuduSinkConfig kuduSinkConfig, CatalogTable catalogTable) {
        this.kuduSinkConfig = kuduSinkConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public String getPluginName() {
        return "Kudu";
    }

    @Override
    public KuduSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new KuduSinkWriter(seaTunnelRowType, kuduSinkConfig);
    }
}
