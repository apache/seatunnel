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

package org.apache.seatunnel.connectors.seatunnel.timeplus.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.timeplus.util.TypeConvertUtil;

import com.timeplus.proton.client.ProtonClient;
import com.timeplus.proton.client.ProtonFormat;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonRequest;
import com.timeplus.proton.client.ProtonResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TimeplusSourceReader implements SourceReader<SeaTunnelRow, TimeplusSourceSplit> {

    private final List<ProtonNode> servers;
    private ProtonClient client;
    private final SeaTunnelRowType rowTypeInfo;
    private final SourceReader.Context readerContext;
    private ProtonRequest<?> request;
    private final String sql;

    private final List<TimeplusSourceSplit> splits;

    TimeplusSourceReader(
            List<ProtonNode> servers,
            SourceReader.Context readerContext,
            SeaTunnelRowType rowTypeInfo,
            String sql) {
        this.servers = servers;
        this.readerContext = readerContext;
        this.rowTypeInfo = rowTypeInfo;
        this.sql = sql;
        this.splits = new ArrayList<>();
    }

    @Override
    public void open() {
        Random random = new Random();
        ProtonNode server = servers.get(random.nextInt(servers.size()));
        client = ProtonClient.newInstance(server.getProtocol());
        request = client.connect(server).format(ProtonFormat.RowBinaryWithNamesAndTypes);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (!splits.isEmpty()) {
            try (ProtonResponse response = this.request.query(sql).executeAndWait()) {
                response.stream()
                        .forEach(
                                record -> {
                                    Object[] values =
                                            new Object[this.rowTypeInfo.getFieldNames().length];
                                    for (int i = 0; i < record.size(); i++) {
                                        if (record.getValue(i).isNullOrEmpty()) {
                                            values[i] = null;
                                        } else {
                                            values[i] =
                                                    TypeConvertUtil.valueUnwrap(
                                                            this.rowTypeInfo.getFieldType(i),
                                                            record.getValue(i));
                                        }
                                    }
                                    output.collect(new SeaTunnelRow(values));
                                });
            }
            this.readerContext.signalNoMoreElement();
            this.splits.clear();
        }
    }

    @Override
    public List<TimeplusSourceSplit> snapshotState(long checkpointId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public void addSplits(List<TimeplusSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
