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
import java.util.Optional;

@Slf4j
public class PrometheusWriter extends HttpSinkWriter {

    // The removed connector-level option key, kept only to detect and warn about a leftover key in
    // an upgraded job config.
    private static final String REMOVED_FLUSH_INTERVAL_KEY = "flush_interval";

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
        // The connector-level `flush_interval` option was removed in favor of the engine-level
        // `sink.flush.interval`. A leftover key in an upgraded job config is silently ignored on a
        // direct job run (only `--check`/`--dry-run` reject unknown keys), so warn here (once per
        // writer instance) to give operators a signal instead of silently dropping periodic
        // flushing.
        if (pluginConfig.getSourceMap().containsKey(REMOVED_FLUSH_INTERVAL_KEY)) {
            log.warn(
                    "The connector option 'flush_interval' has been removed and is ignored. Use the "
                            + "engine-level 'sink.flush.interval' in the job 'env' block instead. "
                            + "Engine-level timer flush is supported only by Zeta; on Spark and "
                            + "Flink there is no periodic flush, so tune 'batch_size' instead.");
        }
        // Opt in to engine-level timer flush. On Zeta the engine invokes this action on the normal
        // Sink input-processing path when a FlushSignal arrives, so there is no connector-owned
        // scheduler thread and no concurrency with write/checkpoint/close. On Spark and Flink the
        // Context does not implement registerFlushAction (it keeps the interface's no-op default),
        // so there is no periodic timer flush there; the buffer is flushed on batch_size and on
        // close(). The null-check is defensive for non-standard/test call sites that may not supply
        // a context.
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

    @Override
    public Optional<Void> prepareCommit() {
        // Flush buffered records on checkpoint. On Spark and Flink the engine-level timer flush
        // (registerFlushAction) keeps the Context's no-op default, so without this the buffer would
        // only be sent on batch_size and on close(). Flushing on checkpoint bounds the buffered
        // window to one checkpoint interval on every engine, matching the sibling FlushSignal sinks
        // (Doris, ClickHouse, Elasticsearch, StarRocks, MongoDB), which flush their buffer here for
        // the non-2PC case and return Optional.empty(). Prometheus remote-write is not
        // transactional, so there is no commit info to return.
        //
        // flush() throws on failure, so a failed checkpoint flush fails the checkpoint instead of
        // silently dropping the batch. On restart the source replays from the last successful
        // checkpoint and re-sends the buffered samples. Whether that replay is harmless depends on
        // the receiver: one that treats a repeated (labels, timestamp) sample as an idempotent
        // upsert absorbs it, but one that rejects duplicate or out-of-order samples may fail the
        // replayed flush, so the delivery guarantee here is at-least-once, not exactly-once.
        flush();
        return Optional.empty();
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
        // Run the final flush and both cleanup steps unconditionally, but keep the first failure as
        // the primary exception and attach later ones with addSuppressed. Otherwise an IOException
        // from closing an HTTP client (thrown from a finally block) would replace the meaningful
        // "Writing records to prometheus failed" exception from the final flush.
        Throwable primary = null;
        try {
            // Send any records still buffered before the writer is closed.
            flush();
        } catch (Throwable t) {
            primary = t;
        }
        try {
            // Close the HttpClientProvider actually used for remote-write (this field shadows the
            // parent's), otherwise it would leak when the writer is closed.
            httpClient.close();
        } catch (Throwable t) {
            primary = addAsPrimaryOrSuppressed(primary, t);
        }
        try {
            super.close();
        } catch (Throwable t) {
            primary = addAsPrimaryOrSuppressed(primary, t);
        }
        if (primary != null) {
            if (primary instanceof IOException) {
                throw (IOException) primary;
            }
            if (primary instanceof RuntimeException) {
                throw (RuntimeException) primary;
            }
            if (primary instanceof Error) {
                throw (Error) primary;
            }
            throw new IOException(primary);
        }
    }

    private static Throwable addAsPrimaryOrSuppressed(Throwable primary, Throwable next) {
        if (primary == null) {
            return next;
        }
        primary.addSuppressed(next);
        return primary;
    }
}
