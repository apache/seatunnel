/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

class TDengineSourceReaderTest {
    Logger logger;
    TDengineSourceReader tDengineSourceReader;

    @BeforeEach
    void setup() {
        tDengineSourceReader = new TDengineSourceReader(null, null);

        List<TDengineSourceSplit> sourceSplits = new ArrayList<>();
        int splitCnt = 100;
        for (int i = 0; i < splitCnt; i++) {
            sourceSplits.add(new TDengineSourceSplit(Integer.toString(i), "select sever_status()"));
        }

        tDengineSourceReader.addSplits(sourceSplits);

        logger = Logger.getLogger("TDengineSourceReaderTest");
    }

    @Test
    void testPoll() throws InterruptedException {
        TestCollector testCollector = new TestCollector();

        int totalSplitCnt = 150;
        ThreadPoolExecutor pool =
                new ThreadPoolExecutor(8, 8, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        pool.execute(
                () -> {
                    for (int i = 0; i < totalSplitCnt; i++) {
                        try {
                            tDengineSourceReader.pollNext(testCollector);
                            Thread.sleep(new Random().nextInt(5));
                        } catch (TDengineConnectorException e) {
                            logger.info("skip create connection!");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        int newSplitCnt = 50;
        int threadCnt = 3;
        for (int i = 0; i < threadCnt; i++) {
            pool.execute(
                    () -> {
                        for (int idx = 0; idx < newSplitCnt; idx++) {
                            logger.info(
                                    String.format(
                                            "%s receive new split",
                                            Thread.currentThread().getName()));
                            tDengineSourceReader.addSplits(
                                    Collections.singletonList(
                                            new TDengineSourceSplit(
                                                    String.format(
                                                            "new_%s",
                                                            Thread.currentThread().getName() + idx),
                                                    "select server_status()")));
                            try {
                                Thread.sleep(new Random().nextInt(5));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }

        pool.awaitTermination(3, TimeUnit.SECONDS);
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        public List<SeaTunnelRow> getRows() {
            return rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return new Object();
        }
    }
}
