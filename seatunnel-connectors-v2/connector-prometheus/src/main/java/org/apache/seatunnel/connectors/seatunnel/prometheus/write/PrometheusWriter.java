package org.apache.seatunnel.connectors.seatunnel.prometheus.write;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

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
import org.apache.seatunnel.connectors.seatunnel.prometheus.write.proto.Remote;
import org.apache.seatunnel.connectors.seatunnel.prometheus.write.proto.Types;

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
    private volatile Exception flushException;
    private final Integer batchSize;
    private PrometheusSinkConfig sinkConfig;
    private final Serializer serializer;
    protected final HttpClientProvider httpClient;

    public PrometheusWriter(
            SeaTunnelRowType seaTunnelRowType, HttpParameter httpParameter, Config pluginConfig) {

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
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Point record = serializer.serialize(element);
        this.write(record);
    }

    public void write(Point record) throws IOException {
        checkFlushException();

        batchList.add(record);
        if (batchSize > 0 && batchList.size() >= batchSize) {
            flush();
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new PrometheusConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Writing records to prometheus failed.",
                    flushException);
        }
    }

    private void flush() {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        try {
            byte[] body = snappy(batchList);
            ByteArrayEntity byteArrayEntity = new ByteArrayEntity(body);
            HttpResponse response =
                    httpClient.doPost(
                            httpParameter.getUrl(), httpParameter.getHeaders(), byteArrayEntity);
            if (HttpStatus.SC_NO_CONTENT == response.getCode()) {
                return;
            }
            log.error(
                    "http client execute exception, http response status code:[{}], content:[{}]",
                    response.getCode(),
                    response.getContent());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        batchList.clear();
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
            for (Map.Entry<String, String> entry : point.getMetricLableMap().entrySet()) {
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
}
