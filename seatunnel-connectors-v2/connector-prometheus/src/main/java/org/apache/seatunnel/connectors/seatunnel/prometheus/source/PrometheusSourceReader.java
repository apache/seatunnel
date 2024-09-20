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
package org.apache.seatunnel.connectors.seatunnel.prometheus.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.prometheus.pojo.InstantPoint;
import org.apache.seatunnel.connectors.seatunnel.prometheus.pojo.RangePoint;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.base.Strings;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.INSTANT_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RANGE_QUERY;

@Slf4j
@Setter
public class PrometheusSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    protected final SingleSplitReaderContext context;
    protected final HttpParameter httpParameter;
    protected HttpClientProvider httpClient;
    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
    };
    private final String contentJson;
    private final String queryType;
    private final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);

    public PrometheusSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            String contentJson,
            String queryType) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.contentJson = contentJson;
        this.queryType = queryType;
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

    private void collect(Collector<SeaTunnelRow> output, String data) throws IOException {
        if (contentJson != null) {
            data = JsonUtils.stringToJsonNode(getPartOfJson(data)).toString();
        }
        switch (queryType) {
            case RANGE_QUERY:
                convertRangePoints(data, output);
                break;
            case INSTANT_QUERY:
                convertInstantPoints(data, output);
                break;
            default:
                throw new PrometheusConnectorException(
                        CommonErrorCode.UNSUPPORTED_METHOD, "unsupported query type");
        }
    }

    private void convertRangePoints(String data, Collector<SeaTunnelRow> output) {
        List<RangePoint> rangePoints = JsonUtils.toList(data, RangePoint.class);
        if (CollectionUtils.isEmpty(rangePoints)) {
            return;
        }
        rangePoints.forEach(
                rangePoint -> {
                    Map<String, String> metric = rangePoint.getMetric();
                    rangePoint
                            .getValues()
                            .forEach(
                                    value -> {
                                        double timestampDouble =
                                                Double.valueOf(String.valueOf(value.get(0)));
                                        // unix transform timestamp
                                        long timestamp = (long) (timestampDouble * 1000);
                                        SeaTunnelRow row =
                                                new SeaTunnelRow(
                                                        new Object[] {
                                                            metric,
                                                            Double.valueOf(
                                                                    String.valueOf(value.get(1))),
                                                            timestamp
                                                        });
                                        output.collect(row);
                                    });
                });
    }

    private void convertInstantPoints(String data, Collector<SeaTunnelRow> output) {
        List<InstantPoint> instantPoints = JsonUtils.toList(data, InstantPoint.class);
        if (CollectionUtils.isEmpty(instantPoints)) {
            return;
        }
        instantPoints.forEach(
                instantPoint -> {
                    double timestampDouble =
                            Double.valueOf(String.valueOf(instantPoint.getValue().get(0)));
                    long timestamp = (long) (timestampDouble * 1000);
                    SeaTunnelRow row =
                            new SeaTunnelRow(
                                    new Object[] {
                                        instantPoint.getMetric(),
                                        Double.valueOf(
                                                String.valueOf(instantPoint.getValue().get(1))),
                                        timestamp
                                    });
                    output.collect(row);
                });
    }

    private String getPartOfJson(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        return JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(contentJson)));
    }

    public void pollAndCollectData(Collector<SeaTunnelRow> output) throws Exception {
        HttpResponse response =
                httpClient.execute(
                        this.httpParameter.getUrl(),
                        this.httpParameter.getMethod().getMethod(),
                        this.httpParameter.getHeaders(),
                        this.httpParameter.getParams(),
                        this.httpParameter.getBody());
        if (response.getCode() >= 200 && response.getCode() <= 207) {
            String content = response.getContent();
            if (!Strings.isNullOrEmpty(content)) {
                if (this.httpParameter.isEnableMultilines()) {
                    StringReader stringReader = new StringReader(content);
                    BufferedReader bufferedReader = new BufferedReader(stringReader);
                    String lineStr;
                    while ((lineStr = bufferedReader.readLine()) != null) {
                        collect(output, lineStr);
                    }
                } else {
                    collect(output, content);
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
