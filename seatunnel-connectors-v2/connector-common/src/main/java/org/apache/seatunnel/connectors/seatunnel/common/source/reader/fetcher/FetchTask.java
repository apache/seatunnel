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

package org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
class FetchTask<E, SplitT extends SourceSplit> implements SplitFetcherTask {
    private static final int OFFER_TIMEOUT_MILLIS = 10000;

    private final SplitReader<E, SplitT> splitReader;
    private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final Consumer<Collection<String>> splitFinishedCallback;
    private final int fetcherIndex;

    @Getter(value = AccessLevel.PRIVATE)
    private volatile boolean wakeup;

    private volatile RecordsWithSplitIds<E> lastRecords;

    @Override
    public void run() throws IOException {
        try {
            if (!isWakeup() && lastRecords == null) {
                lastRecords = splitReader.fetch();
                log.debug("Fetch records from split fetcher {}", fetcherIndex);
            }

            if (!isWakeup()) {
                if (elementsQueue.offer(lastRecords, OFFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    if (!lastRecords.finishedSplits().isEmpty()) {
                        splitFinishedCallback.accept(lastRecords.finishedSplits());
                    }
                    lastRecords = null;
                    log.debug("Enqueued records from split fetcher {}", fetcherIndex);
                } else {
                    log.debug(
                            "Enqueuing timed out in split fetcher {}, queue is blocked",
                            fetcherIndex);
                }
            }
        } catch (IOException | InterruptedException e) {
            // this should only happen on shutdown
            throw new IOException("Source fetch execution was fail", e);
        } finally {
            // clean up the potential wakeup effect.
            if (isWakeup()) {
                wakeup = false;
            }
        }
    }

    @Override
    public void wakeUp() {
        // Set the wakeup flag first.
        wakeup = true;

        if (lastRecords == null) {
            splitReader.wakeUp();
        } else {
            // interrupt enqueuing the records
            // or waitting records offer into queue timeout, see {@link #run()}
        }
    }
}
