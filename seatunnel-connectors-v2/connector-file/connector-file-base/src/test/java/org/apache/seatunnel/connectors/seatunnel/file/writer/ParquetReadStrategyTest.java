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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.file.source.reader.ParquetReadStrategy;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;

public class ParquetReadStrategyTest {
    @Test
    public void testParquetRead() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/test.parquet");
        assert resource != null;
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        parquetReadStrategy.init(null);
        SeaTunnelRowType seaTunnelRowTypeInfo = parquetReadStrategy.getSeaTunnelRowTypeInfo(null, path);
        assert seaTunnelRowTypeInfo != null;
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, testCollector);
    }

    public static class TestCollector implements Collector<SeaTunnelRow> {

        @SuppressWarnings("checkstyle:RegexpSingleline")
        @Override
        public void collect(SeaTunnelRow record) {
            System.out.println(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }
}
