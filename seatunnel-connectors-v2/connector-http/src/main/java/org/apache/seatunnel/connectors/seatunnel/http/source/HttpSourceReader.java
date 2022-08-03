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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import static org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse.STATUS_OK;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class HttpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceReader.class);
    private final SingleSplitReaderContext context;
    private final HttpParameter httpParameter;
    private HttpClientProvider httpClient;

    public HttpSourceReader(HttpParameter httpParameter, SingleSplitReaderContext context) {
        this.context = context;
        this.httpParameter = httpParameter;
    }

    @Override
    public void open() {
        httpClient = HttpClientProvider.getInstance();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            HttpResponse response = httpClient.execute(this.httpParameter.getUrl(), this.httpParameter.getMethod(), this.httpParameter.getHeaders(), this.httpParameter.getParams());
            if (STATUS_OK == response.getCode()) {
                output.collect(new SeaTunnelRow(new Object[] {response.getContent()}));
                return;
            }
            LOGGER.error("http client execute exception, http response status code:[{}], content:[{}]", response.getCode(), response.getContent());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                LOGGER.info("Closed the bounded http source");
                context.signalNoMoreElement();
            }
        }
    }
}
