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

package org.apache.seatunnel.connectors.seatunnel.tablestore.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.tablestore.serialize.SeaTunnelRowSerializer;

import java.io.IOException;
import java.util.Optional;

public class TablestoreWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final TablestoreSinkClient tablestoreSinkClient;
    private final SeaTunnelRowSerializer serializer;

    public TablestoreWriter(
            TablestoreOptions tablestoreOptions, SeaTunnelRowType seaTunnelRowType) {
        tablestoreSinkClient = new TablestoreSinkClient(tablestoreOptions, seaTunnelRowType);
        serializer = new DefaultSeaTunnelRowSerializer(seaTunnelRowType, tablestoreOptions);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tablestoreSinkClient.write(serializer.serialize(element));
    }

    @Override
    public void close() throws IOException {
        tablestoreSinkClient.close();
    }

    @Override
    public Optional<Void> prepareCommit() {
        tablestoreSinkClient.flush();
        return super.prepareCommit();
    }
}
