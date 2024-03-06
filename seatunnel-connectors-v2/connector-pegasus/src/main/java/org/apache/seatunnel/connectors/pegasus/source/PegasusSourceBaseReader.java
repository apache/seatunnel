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

package org.apache.seatunnel.connectors.pegasus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.pegasus.sink.PegasusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusClientFactory;
import org.apache.pegasus.client.PegasusClientInterface;
import org.apache.pegasus.client.PegasusTableInterface;

import java.io.IOException;

public abstract class PegasusSourceBaseReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    protected final PegasusTableInterface table;
    protected final PegasusClientInterface client;
    protected final SingleSplitReaderContext context;

    public PegasusSourceBaseReader(Config pluginConfig, SingleSplitReaderContext context)
            throws PException {
        ClientOptions clientOptions =
                ClientOptions.builder()
                        .metaServers(pluginConfig.getString(PegasusSinkConfig.META_SERVER.key()))
                        // TODO
                        // .operationTimeout(1000)
                        .build();
        client = PegasusClientFactory.getSingletonClient(clientOptions);
        table = client.openTable(pluginConfig.getString(PegasusSinkConfig.TABLE.key()));
        this.context = context;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {
        client.close();
    }
}
