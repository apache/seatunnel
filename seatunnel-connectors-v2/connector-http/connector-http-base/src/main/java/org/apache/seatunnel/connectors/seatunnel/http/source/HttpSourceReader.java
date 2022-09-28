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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class HttpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceReader.class);
    protected final SingleSplitReaderContext context;
    protected final HttpParameter httpParameter;
    protected HttpClientProvider httpClient;
    private final DeserializationCollector deserializationCollector;

    public HttpSourceReader(HttpParameter httpParameter, SingleSplitReaderContext context, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(httpParameter);
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
            if (HttpResponse.STATUS_OK == response.getCode()) {
                String content = response.getContent();
                if (!Strings.isNullOrEmpty(content)) {
                    deserializationCollector.collect(content.getBytes(), output);
                }
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
            } else {
                if (httpParameter.getPollIntervalMillis() > 0) {
                    Thread.sleep(httpParameter.getPollIntervalMillis());
                }
            }
        }
    }
}
