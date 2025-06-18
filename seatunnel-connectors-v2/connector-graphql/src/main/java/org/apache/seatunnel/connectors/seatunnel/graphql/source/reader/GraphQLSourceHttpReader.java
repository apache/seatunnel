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

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.graphql.config.GraphQLSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.graphql.util.GraphQLUtil;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.DeserializationCollector;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Objects;

@Slf4j
public class GraphQLSourceHttpReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    protected final GraphQLSourceParameter graphQLSourceParameter;
    protected final HttpParameter httpParameter;

    protected final SingleSplitReaderContext context;

    protected HttpClientProvider httpClient;
    private final String contentJson;

    private final DeserializationCollector deserializationCollector;

    public GraphQLSourceHttpReader(
            GraphQLSourceParameter graphQLSourceParameter,
            SingleSplitReaderContext context,
            String contentJson,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.context = context;
        this.graphQLSourceParameter = graphQLSourceParameter;
        this.httpParameter = graphQLSourceParameter.getHttpParameter();
        this.contentJson = contentJson;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
    }

    @Override
    public void open() throws Exception {
        httpClient = new HttpClientProvider(graphQLSourceParameter.getHttpParameter());
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            internalPollNext(output);
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            pollAndCollectData(output);
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded http source");
                context.signalNoMoreElement();
            } else {
                if (httpParameter.getPollIntervalMillis() > 0) {
                    Thread.sleep(httpParameter.getPollIntervalMillis());
                }
            }
        }
    }

    public void pollAndCollectData(Collector<SeaTunnelRow> output) throws Exception {
        HttpResponse response =
                httpClient.execute(
                        this.httpParameter.getUrl(),
                        this.httpParameter.getMethod().getMethod(),
                        this.httpParameter.getHeaders(),
                        this.httpParameter.getParams(),
                        this.httpParameter.getBody(),
                        this.httpParameter.isKeepParamsAsForm());
        if (response.getCode() >= 200 && response.getCode() <= 207) {
            String content = response.getContent();
            if (!Strings.isNullOrEmpty(content)) {
                if (this.httpParameter.isEnableMultilines()) {
                    StringReader stringReader = new StringReader(content);
                    BufferedReader bufferedReader = new BufferedReader(stringReader);
                    String lineStr;
                    while ((lineStr = bufferedReader.readLine()) != null) {
                        GraphQLUtil.collect(deserializationCollector, lineStr, contentJson, output);
                    }
                } else {
                    GraphQLUtil.collect(deserializationCollector, content, contentJson, output);
                }
            }
            log.debug(
                    "http client execute success request param:[{}], http response status code:[{}], content:[{}]",
                    httpParameter.getParams(),
                    response.getCode(),
                    response.getContent());
        } else {
            String msg =
                    String.format(
                            "http client execute exception, http response status code:[%s], content:[%s]",
                            response.getCode(), response.getContent());
            throw new HttpConnectorException(HttpConnectorErrorCode.REQUEST_FAILED, msg);
        }
    }
}
