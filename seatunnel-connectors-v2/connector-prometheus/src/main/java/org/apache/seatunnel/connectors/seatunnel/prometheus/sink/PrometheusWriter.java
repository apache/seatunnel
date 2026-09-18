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
    // Retry configuration for the remote-write flush, sourced from the shared HTTP retry options
    // (retry / retry_backoff_multiplier_ms / retry_backoff_max_ms). The base HttpClientProvider
    // retries transport IOExceptions with these; the writer reuses the same values to retry
    // retryable HTTP statuses (5xx/429), which the base retryer cannot see.
    private final int retry;
    private final int retryBackoffMultiplierMs;
    private final int retryBackoffMaxMs;
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
        this.retry = Math.max(0, httpParameter.getRetry());
        this.retryBackoffMultiplierMs = httpParameter.getRetryBackoffMultiplierMillis();
        this.retryBackoffMaxMs = httpParameter.getRetryBackoffMaxMillis();
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
            final byte[] body;
            try {
                body = snappy(batchList);
            } catch (IOException e) {
                // Encoding failure is a bug, not a transient error, so do not retry it.
                throw new PrometheusConnectorException(
                        CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                        "Failed to compress records for the prometheus remote-write request.",
                        e);
            }
            // The base HttpClientProvider already retries transport IOExceptions (using the same
            // retry / retry_backoff_* options). Here we additionally retry retryable HTTP statuses
            // (5xx and 429), which the base retryer cannot see because they come back as responses
            // rather than exceptions. Other 4xx fail fast, and a duplicate/out-of-order 400 is
            // treated as delivered (see sendOnce). After the retries are exhausted flush() throws,
            // so a genuine outage fails the caller (a checkpoint via prepareCommit, batch_size, the
            // timer flush, or close()) instead of silently dropping the batch.
            //
            // `retry` is the total attempt budget here, matching the base transport
            // retryer (which uses stopAfterAttempt(retry)). The base uses a Fibonacci
            // backoff while this status path uses a capped exponential backoff, both
            // bounded by the retry_backoff_* options.
            int maxAttempts = Math.max(1, retry);
            for (int attempt = 1; ; attempt++) {
                try {
                    sendOnce(body);
                    batchList.clear();
                    return;
                } catch (RetryableFlushException e) {
                    if (attempt >= maxAttempts) {
                        throw new PrometheusConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                String.format(
                                        "Writing records to prometheus failed after %d attempt(s).",
                                        attempt),
                                e);
                    }
                    log.warn(
                            "Prometheus remote-write attempt {}/{} failed with a retryable response, "
                                    + "retrying: {}",
                            attempt,
                            maxAttempts,
                            e.getMessage());
                    sleepBeforeRetry(attempt);
                }
            }
        }
    }

    private void sleepBeforeRetry(int attempt) {
        // Exponential backoff (multiplier * 2^(attempt-1)) capped at retry_backoff_max_ms. The
        // attempt >= 31 guard keeps the left shift from overflowing on a large attempt count.
        long backoff =
                attempt >= 31
                        ? retryBackoffMaxMs
                        : Math.min(
                                (long) retryBackoffMultiplierMs << (attempt - 1),
                                retryBackoffMaxMs);
        if (backoff <= 0) {
            return;
        }
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new PrometheusConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Interrupted while backing off before a prometheus remote-write retry.",
                    ie);
        }
    }

    /**
     * Send the current batch once. Returns normally when the batch is delivered (HTTP 204) or when
     * the receiver rejects it as a duplicate/out-of-order sample (HTTP 400 with a matching body),
     * which per the remote-write spec must not be retried and is safe to treat as delivered on a
     * replay. Throws {@link RetryableFlushException} for a retryable HTTP status (5xx and 429) so
     * flush() retries it, and a non-retryable {@link PrometheusConnectorException} for a
     * transport-level failure (already retried by the base client) or any other response.
     */
    private void sendOnce(byte[] body) {
        HttpResponse response;
        try {
            response =
                    httpClient.doPost(
                            httpParameter.getUrl(),
                            httpParameter.getHeaders(),
                            new ByteArrayEntity(body));
        } catch (Exception e) {
            // Transport-level failure (connect/read timeout, reset, DNS, etc.). The base
            // HttpClientProvider has already applied its own IOException retries via the retry /
            // retry_backoff_* options, so surface it here rather than retrying a second time.
            throw new PrometheusConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Prometheus remote-write request failed to complete.",
                    e);
        }
        int code = response.getCode();
        if (HttpStatus.SC_NO_CONTENT == code) {
            return;
        }
        if (HttpStatus.SC_BAD_REQUEST == code
                && indicatesDuplicateOrOutOfOrder(response.getContent())) {
            // A replay after a restore can re-send samples the receiver already has or that are
            // older than its head for the series; the receiver returns 400 for these. Per the
            // remote-write spec 4xx must not be retried, and failing here would loop the job on the
            // same batch, so treat it as delivered. The match on the response body is best effort
            // (see indicatesDuplicateOrOutOfOrder), so log at WARN: it discards an error response,
            // and a false positive would drop the batch rather than resend it.
            log.warn(
                    "Prometheus returned HTTP 400 whose body matches a duplicate/out-of-order "
                            + "rejection; treating the batch as delivered. If this receiver returns "
                            + "400 for an unrelated reason, the batch would be dropped. content:[{}]",
                    response.getContent());
            return;
        }
        if (isRetryableStatus(code)) {
            throw new RetryableFlushException(
                    String.format(
                            "Writing records to prometheus failed with retryable http status "
                                    + "code:[%d], content:[%s]",
                            code, response.getContent()));
        }
        throw new PrometheusConnectorException(
                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                String.format(
                        "Writing records to prometheus failed, http response status code:[%d], content:[%s]",
                        code, response.getContent()));
    }

    private static boolean isRetryableStatus(int code) {
        // Per the remote-write spec 5xx should be retried; 429 (Too Many Requests) is a transient
        // backpressure signal, so retry it too.
        return code == 429 || (code >= 500 && code <= 599);
    }

    private static boolean indicatesDuplicateOrOutOfOrder(String content) {
        if (content == null) {
            return false;
        }
        String lower = content.toLowerCase();
        return lower.contains("duplicate sample")
                || lower.contains("out of order")
                || lower.contains("out-of-order")
                || lower.contains("too old")
                || lower.contains("out of bounds");
    }

    /** Internal marker for a flush failure that should be retried. */
    private static class RetryableFlushException extends RuntimeException {
        RetryableFlushException(String message) {
            super(message);
        }

        RetryableFlushException(String message, Throwable cause) {
            super(message, cause);
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
