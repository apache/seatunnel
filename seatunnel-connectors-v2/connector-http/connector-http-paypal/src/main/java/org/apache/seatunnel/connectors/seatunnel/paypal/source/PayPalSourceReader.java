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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.io.IOException;

final class PayPalSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final PayPalConfig config;
    private final SingleSplitReaderContext context;
    private volatile boolean closed;
    private PayPalClient client;

    PayPalSourceReader(PayPalConfig config, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
    }

    /** Create worker-local HTTP resources; credentials and clients never enter split state. */
    @Override
    public synchronized void open() {
        if (client != null) {
            throw PayPalResponse.failure("Reader already opened");
        }
        if (closed) {
            throw PayPalResponse.failure("Reader closed");
        }
        client = new PayPalClient(config);
    }

    /** Replay the bounded report on recovery; page offsets are not durable snapshots. */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        int total = -1;
        int pages = -1;
        String account = null;
        for (int page = 1; ; page++) {
            if (closed) {
                throw PayPalResponse.failure("Reader closed");
            }
            PayPalResponse response = new PayPalResponse(client.page(page), config, page);
            if (total < 0) {
                total = response.totalItems;
                pages = response.totalPages;
                account = response.account;
            } else if (total != response.totalItems
                    || pages != response.totalPages
                    || !account.equals(response.account)) {
                throw PayPalResponse.failure(
                        "Report totals or account changed between pages; no stable remote snapshot is guaranteed");
            }
            for (SeaTunnelRow row : response.rows) {
                if (closed) {
                    throw PayPalResponse.failure("Reader closed");
                }
                output.collect(row);
            }
            if (page >= pages) {
                break;
            }
        }
        if (closed) {
            throw PayPalResponse.failure("Reader closed");
        }
        context.signalNoMoreElement();
    }

    /** Abort in-flight HTTP and wake retry waits before releasing worker-local resources. */
    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (client != null) {
            client.close();
        }
    }
}
