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
package org.apache.seatunnel.connectors.seatunnel.prometheus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.prometheus.serialize.PrometheusSerializer;
import org.apache.seatunnel.connectors.seatunnel.prometheus.serialize.Serializer;
import org.apache.seatunnel.connectors.seatunnel.prometheus.sink.proto.Remote;
import org.apache.seatunnel.connectors.seatunnel.prometheus.sink.proto.Types;

import org.apache.http.HttpStatus;
import org.apache.http.entity.ByteArrayEntity;

import org.xerial.snappy.Snappy;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class PrometheusWriter extends HttpSinkWriter {
    private final List<Point> batchList;
    private final Integer batchSize;
    private final PrometheusSinkConfig sinkConfig;
    private final Serializer serializer;
    protected final HttpClientProvider httpClient;

    public PrometheusWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            ReadonlyConfig pluginConfig,
            SinkWriter.Context context) {

        super(seaTunnelRowType, httpParameter);
        this.batchList = new ArrayList<>();
        this.sinkConfig = PrometheusSinkConfig.loadConfig(pluginConfig);
        this.batchSize = sinkConfig.getBatchSize();
        this.serializer =
                new PrometheusSerializer(
                        seaTunnelRowType,
                        sinkConfig.getKeyTimestamp(),
                        sinkConfig.getKeyLabel(),
                        sinkConfig.getKeyValue());
        this.httpClient = new HttpClientProvider(httpParameter);
        // Opt in to engine-level timer flush. The engine invokes this action on the normal Sink
        // input-processing path when a FlushSignal arrives, so no connector-owned scheduler thread
        // is needed and there is no concurrency with write/checkpoint/close.
        if (context != null) {
            context.registerFlushAction(this::flush);
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        Point record = serializer.serialize(element);
        this.write(record);
    }

    public void write(Point record) {
        synchronized (batchList) {
            batchList.add(record);
            if (batchSize > 0 && batchList.size() >= batchSize) {
                flush();
            }
        }
    }

    private void flush() {
        synchronized (batchList) {
            if (batchList.isEmpty()) {
                return;
            }
            try {
                byte[] body = snappy(batchList);
                ByteArrayEntity byteArrayEntity = new ByteArrayEntity(body);
                HttpResponse response =
                        httpClient.doPost(
                                httpParameter.getUrl(),
                                httpParameter.getHeaders(),
                                byteArrayEntity);
                if (HttpStatus.SC_NO_CONTENT == response.getCode()) {
                    batchList.clear();
                    return;
                }
                // Propagate the failure to the engine instead of silently dropping the batch, so a
                // flush that did not succeed is not treated as a successful flush.
                throw new PrometheusConnectorException(
                        CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                        String.format(
                                "Writing records to prometheus failed, http response status code:[%d], content:[%s]",
                                response.getCode(), response.getContent()));
            } catch (PrometheusConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new PrometheusConnectorException(
                        CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                        "Writing records to prometheus failed.",
                        e);
            }
        }
    }

    /**
     * snappy data
     *
     * @param points list of series data
     * @return byte data
     * @throws IOException IOException
     */
    private byte[] snappy(List<Point> points) throws IOException {
        Remote.WriteRequest writeRequest = createRemoteWriteRequest(points);
        byte[] serializedData = writeRequest.toByteArray();
        byte[] compressedData = Snappy.compress(serializedData);
        return compressedData;
    }

    /**
     * create Remote Write Request
     *
     * @param points list of series data
     * @return Remote.WriteRequest
     */
    private Remote.WriteRequest createRemoteWriteRequest(List<Point> points) {
        Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
        for (Point point : points) {
            List<Types.Label> labels = new ArrayList<>();
            Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();
            for (Map.Entry<String, String> entry : point.getMetric().entrySet()) {
                Types.Label label =
                        Types.Label.newBuilder()
                                .setName(entry.getKey())
                                .setValue(entry.getValue())
                                .build();
                labels.add(label);
            }
            Types.Sample sample =
                    Types.Sample.newBuilder()
                            .setTimestamp(point.getTimestamp())
                            .setValue(point.getValue())
                            .build();
            timeSeriesBuilder.addAllLabels(labels);
            timeSeriesBuilder.addSamples(sample);
            writeRequestBuilder.addTimeseries(timeSeriesBuilder);
        }
        return writeRequestBuilder.build();
    }

    @Override
    public void close() throws IOException {
        try {
            // Send any records still buffered before the writer is closed.
            flush();
        } finally {
            super.close();
        }
    }
}
