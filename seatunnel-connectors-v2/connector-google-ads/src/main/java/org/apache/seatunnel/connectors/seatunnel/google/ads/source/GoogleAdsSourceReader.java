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

package org.apache.seatunnel.connectors.seatunnel.google.ads.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.google.ads.client.GoogleAdsClient;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsTableConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class GoogleAdsSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final GoogleAdsParameters params;
    private final List<GoogleAdsTableConfig> tableConfigs;
    private final SingleSplitReaderContext readerContext;
    private GoogleAdsClient client;

    GoogleAdsSourceReader(
            GoogleAdsParameters params,
            List<GoogleAdsTableConfig> tableConfigs,
            SingleSplitReaderContext readerContext) {
        this.params = params;
        this.tableConfigs = tableConfigs;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        client = new GoogleAdsClient(params);
        client.authenticate();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Single-pass bounded read for the assigned split. For each configured table, runs its GAQL
     * query and forwards each already-typed Object[] downstream as a table-tagged SeaTunnelRow;
     * rows stream through page by page without buffering the whole result set. After every table
     * has drained, signals no-more-elements so the framework can close the split.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            for (GoogleAdsTableConfig tableConfig : tableConfigs) {
                SeaTunnelRowType rowType = tableConfig.getCatalogTable().getSeaTunnelRowType();
                String tableId = tableConfig.getTableId();
                log.info(
                        "Reading rows from Google Ads resource {} for customer {}",
                        tableConfig.getResource(),
                        tableConfig.getCustomerId());
                client.search(
                        tableConfig.getCustomerId(),
                        tableConfig.getGaql(),
                        tableConfig.getFieldPaths(),
                        rowType,
                        values -> {
                            SeaTunnelRow row = new SeaTunnelRow(values);
                            row.setTableId(tableId);
                            output.collect(row);
                        });
            }
        } finally {
            readerContext.signalNoMoreElement();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
