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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReport.failure;

final class TikTokAdsSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final TikTokAdsConfig config;
    private final SingleSplitReaderContext context;
    private volatile TikTokAdsClient client;
    private boolean closed;

    TikTokAdsSourceReader(TikTokAdsConfig config, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
    }

    /** Create worker-local resources only; the source remains serializable. */
    @Override
    public synchronized void open() throws IOException {
        if (closed) {
            throw failure("reader closed");
        }
        if (client == null) {
            client = new TikTokAdsClient(config);
        }
    }

    /**
     * Read one bounded report. Restore intentionally replays page one, not a mutable page offset.
     */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        TikTokAdsClient current = client;
        if (current == null) {
            throw failure("reader is not open");
        }
        current.startReport();
        Set<List<String>> seen = new HashSet<>();
        int total = -1;
        int pages = -1;
        int emitted = 0;
        for (int number = 1; ; number++) {
            TikTokAdsClient.Response response = fetchWithRetry(current, number);
            TikTokAdsReport.Page page = TikTokAdsReport.parse(response.body, number, config, seen);
            if (total != -1 && (total != page.total || pages != page.totalPages)) {
                throw failure("report totals changed during pagination");
            }
            total = page.total;
            pages = page.totalPages;
            for (SeaTunnelRow row : page.rows) {
                current.checkActive();
                output.collect(row);
                emitted++;
            }
            if (number >= pages) {
                if (emitted != total) {
                    throw failure("incomplete report");
                }
                current.checkActive();
                context.signalNoMoreElement();
                return;
            }
        }
    }

    private TikTokAdsClient.Response fetchWithRetry(TikTokAdsClient current, int page)
            throws IOException {
        for (int attempt = 0; ; attempt++) {
            TikTokAdsClient.Response response = current.fetch(page);
            if (response.status == 200) {
                return response;
            }
            boolean transientStatus =
                    response.status == 429
                            || response.status == 500
                            || response.status == 502
                            || response.status == 503
                            || response.status == 504;
            if (!transientStatus || attempt >= config.getRetries()) {
                throw failure("HTTP " + response.status + "; request failed");
            }
            current.awaitRetry(retryDelay(response.retryAfter, attempt, config.getRetryWait()));
        }
    }

    static long retryDelay(String retryAfter, int attempt, int maxWait) throws IOException {
        long delay = Math.min(maxWait, 1000L << attempt);
        if (retryAfter != null) {
            try {
                long requested =
                        retryAfter.matches("[0-9]{1,9}")
                                ? Math.multiplyExact(Long.parseLong(retryAfter), 1000L)
                                : Math.max(
                                        0,
                                        ZonedDateTime.parse(
                                                                retryAfter,
                                                                DateTimeFormatter
                                                                        .RFC_1123_DATE_TIME)
                                                        .toInstant()
                                                        .toEpochMilli()
                                                - System.currentTimeMillis());
                if (requested > maxWait) {
                    throw failure("Retry-After exceeds max_retry_wait_ms");
                }
                delay = Math.max(delay, requested);
            } catch (DateTimeParseException | ArithmeticException e) {
                throw failure("invalid Retry-After header");
            }
        }
        return delay;
    }

    /** Abort active IO and wake retry waits even when pollNext holds the checkpoint lock. */
    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (client != null) {
            client.close();
        }
    }
}
