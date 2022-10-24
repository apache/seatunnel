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

package org.apache.seatunnel.connectors.seatunnel.tikv.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.ClientSession;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVDataType;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;

import org.tikv.raw.RawKVClient;

import java.util.Arrays;
import java.util.List;

public class TiKVSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final ClientSession clientSession;
    private final TiKVParameters tikvParameters;
    private final SeaTunnelRowType seaTunnelRowType;

    public TiKVSinkWriter(SeaTunnelRowType seaTunnelRowType, TiKVParameters tikvParameters) {
        this.tikvParameters = tikvParameters;
        this.clientSession = new ClientSession(tikvParameters);
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        RawKVClient client = clientSession.session.createRawClient();
        TiKVDataType tikvDataType = tikvParameters.getTikvDataType();

        String data = seaTunnelRow.getField(0).toString();
        String keyword = tikvParameters.getKeyword();
        List<String> fields = Arrays.asList(seaTunnelRowType.getFieldNames());
        String key;
        if (fields.contains(keyword)) {
            key = seaTunnelRow.getField(fields.indexOf(keyword)).toString();
        } else {
            key = keyword;
        }
        tikvDataType.set(client, key, data);
    }

    @Override
    public void close() {
        try {
            clientSession.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
