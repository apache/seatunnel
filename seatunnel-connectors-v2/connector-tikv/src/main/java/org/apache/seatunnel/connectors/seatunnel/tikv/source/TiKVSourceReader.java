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

package org.apache.seatunnel.connectors.seatunnel.tikv.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.ClientSession;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;

import org.tikv.raw.RawKVClient;

import java.io.IOException;
import java.util.Objects;

public class TiKVSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private ClientSession clientSession;

    private final TiKVParameters tikvParameters;

    private final SingleSplitReaderContext context;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public TiKVSourceReader(TiKVParameters tikvParameters,
                            SingleSplitReaderContext context,
                            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.tikvParameters = tikvParameters;
        this.context = context;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open() {
        this.clientSession = new ClientSession(tikvParameters);
    }

    @Override
    public void close() {
        if (Objects.nonNull(clientSession)) {
            try {
                clientSession.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        // create session client
        RawKVClient client = clientSession.session.createRawClient();
        // by the key get the value
        tikvParameters.getTikvDataType()
            .get(client, tikvParameters)
            .forEach(value -> collectResult(output, value));
        // termination signal
        context.signalNoMoreElement();
    }

    private void collectResult(Collector<SeaTunnelRow> output, String value) {
        if (deserializationSchema == null) {
            output.collect(new SeaTunnelRow(new Object[]{value}));
        } else {
            try {
                deserializationSchema.deserialize(value.getBytes(), output);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
