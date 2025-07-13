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

package org.apache.seatunnel.connectors.seatunnel.graphql.source.reader;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.graphql.config.GraphQLSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.graphql.util.GraphQLUtil;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.source.DeserializationCollector;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class GraphQLSourceSocketReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    protected final GraphQLSourceParameter graphQLSourceParameter;
    private LinkedBlockingQueue<String> buffer;
    private GraphQLWebSocket graphQLWebSocket;

    protected final HttpParameter httpParameter;

    protected final SingleSplitReaderContext context;
    private final String contentJson;
    private final DeserializationCollector deserializationCollector;

    public GraphQLSourceSocketReader(
            GraphQLSourceParameter graphQLSourceParameter,
            SingleSplitReaderContext context,
            String contentJson,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.context = context;
        this.graphQLSourceParameter = graphQLSourceParameter;
        this.httpParameter = graphQLSourceParameter.getHttpParameter();
        this.contentJson = contentJson;
        this.buffer = new LinkedBlockingQueue<>();
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
    }

    @Override
    public void open() throws Exception {
        graphQLWebSocket = new GraphQLWebSocket(buffer, graphQLSourceParameter);
        graphQLWebSocket.start();
    }

    @Override
    public void close() throws IOException {
        if (graphQLWebSocket != null) {
            graphQLWebSocket.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        String data = buffer.poll();
        GraphQLUtil.collect(deserializationCollector, data, contentJson, output);
    }
}
