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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.exception.OpenMldbConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class OpenMldbSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final List<OpenMldbParameters> tableParameters;
    private final List<SeaTunnelRowType> rowTypes;
    private final List<String> tableIds;
    private final SingleSplitReaderContext readerContext;
    private OpenMldbReadClient client;
    private boolean finished;

    public OpenMldbSourceReader(
            OpenMldbParameters openMldbParameters,
            SeaTunnelRowType seaTunnelRowType,
            SingleSplitReaderContext readerContext) {
        this.tableParameters = Collections.singletonList(openMldbParameters);
        this.rowTypes = Collections.singletonList(seaTunnelRowType);
        this.tableIds = Collections.singletonList(null);
        this.readerContext = readerContext;
    }

    OpenMldbSourceReader(
            List<OpenMldbParameters> tableParameters,
            List<CatalogTable> catalogTables,
            boolean multiTable,
            SingleSplitReaderContext readerContext) {
        this.tableParameters = new ArrayList<>(tableParameters);
        this.rowTypes = new ArrayList<>();
        this.tableIds = new ArrayList<>();
        for (CatalogTable table : catalogTables) {
            rowTypes.add(table.getSeaTunnelRowType());
            tableIds.add(multiTable ? table.getTableId().toTablePath().toString() : null);
        }
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        client = new OpenMldbReadClient(tableParameters.get(0));
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (finished) {
            return;
        }
        for (int i = 0; i < tableParameters.size(); i++) {
            readTable(output, tableParameters.get(i), rowTypes.get(i), tableIds.get(i));
        }
        if (Boundedness.BOUNDED.equals(readerContext.getBoundedness())) {
            finished = true;
            log.info("Finished reading the bounded OpenMldb source");
            readerContext.signalNoMoreElement();
        }
    }

    private void readTable(
            Collector<SeaTunnelRow> output,
            OpenMldbParameters parameters,
            SeaTunnelRowType rowType,
            String tableId)
            throws SQLException {
        try (OpenMldbReadClient.Query query =
                client.execute(
                        parameters.getDatabase(), parameters.getSql(), rowType, tableId != null)) {
            while (query.next()) {
                SeaTunnelRow row = query.readRow();
                if (tableId != null) {
                    row.setTableId(tableId);
                }
                output.collect(row);
            }
        } catch (SQLException | RuntimeException e) {
            throw new OpenMldbConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                    "Failed to read OpenMldb table '"
                            + (tableId == null ? parameters.getDatabase() : tableId)
                            + "'",
                    e);
        }
    }
}
