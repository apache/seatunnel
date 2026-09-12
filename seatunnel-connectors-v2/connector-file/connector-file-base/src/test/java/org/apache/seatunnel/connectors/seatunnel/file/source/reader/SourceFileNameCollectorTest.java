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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import java.util.ArrayList;
import java.util.List;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SourceFileNameCollectorTest {

    @Test
    void extractsFileNameFromLocalAndObjectStoragePaths() {
        Assertions.assertEquals(
                "orders.csv", SourceFileNameCollector.extractFileName("/data/in/orders.csv"));
        Assertions.assertEquals(
                "orders.csv", SourceFileNameCollector.extractFileName("C:\\data\\in\\orders.csv"));
        Assertions.assertEquals(
                "orders.csv",
                SourceFileNameCollector.extractFileName(
                        "abfss://files@account.dfs.core.windows.net/in/orders.csv"));
    }

    @Test
    void addsStableSourceFileMetadataToRows() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> collector =
                SourceFileNameCollector.wrap(
                        new ListCollector(rows), "/data/in/customer-orders.csv");

        collector.collect(new SeaTunnelRow(new Object[] {1}));
        collector.collect(new SeaTunnelRow(new Object[] {2}));

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(
                "customer-orders.csv",
                rows.get(0).getOptions().get(SourceFileNameCollector.SOURCE_FILE_NAME));
        Assertions.assertEquals(
                rows.get(0).getOptions().get(SourceFileNameCollector.SOURCE_FILE_ID),
                rows.get(1).getOptions().get(SourceFileNameCollector.SOURCE_FILE_ID));
    }

    private static class ListCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows;

        private ListCollector(List<SeaTunnelRow> rows) {
            this.rows = rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
