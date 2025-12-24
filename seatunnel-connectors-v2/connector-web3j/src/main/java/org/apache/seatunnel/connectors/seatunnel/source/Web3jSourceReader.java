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

package org.apache.seatunnel.connectors.seatunnel.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Web3jSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final Web3jSourceParameter parameter;
    private final SingleSplitReaderContext context;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Web3j web3;

    Web3jSourceReader(Web3jSourceParameter parameter, SingleSplitReaderContext context) {
        this.parameter = parameter;
        this.context = context;
    }

    @Override
    public void open() throws Exception {
        web3 = Web3j.build(new HttpService(this.parameter.getUrl()));
        log.info("connect Web3j server, url:[{}] ", this.parameter.getUrl());
    }

    @Override
    public void close() throws IOException {
        if (web3 != null) {
            web3.shutdown();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        web3.ethBlockNumber()
                .flowable()
                .subscribe(
                        blockNumber -> {
                            Map<String, Object> data = new HashMap<>();
                            data.put("timestamp", Instant.now().toString());
                            data.put("blockNumber", blockNumber.getBlockNumber());

                            String json = OBJECT_MAPPER.writeValueAsString(data);

                            output.collect(new SeaTunnelRow(new Object[] {json}));

                            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                                // signal to the source that we have reached the end of the data.
                                context.signalNoMoreElement();
                            }
                        });
    }
}
