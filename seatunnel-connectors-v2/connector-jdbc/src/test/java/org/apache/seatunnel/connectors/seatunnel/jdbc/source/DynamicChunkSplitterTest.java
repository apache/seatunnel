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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicChunkSplitterTest {

    @Test
    public void testGenerateSplitQuerySQL() {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:postgresql://localhost:5432/test")
                                        .driverName("org.postgresql.Driver")
                                        .build())
                        .build();
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);

        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db1", "schema1", "table1"),
                        "split1",
                        null,
                        "id",
                        BasicType.INT_TYPE,
                        1,
                        10);
        String splitQuerySQL = splitter.createDynamicSplitQuerySQL(split);
        Assertions.assertEquals(
                "SELECT * FROM \"db1\".\"schema1\".\"table1\" WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitQuerySQL);

        split =
                new JdbcSourceSplit(
                        TablePath.of("db1", "schema1", "table1"),
                        "split1",
                        "select * from table1",
                        "id",
                        BasicType.INT_TYPE,
                        1,
                        10);
        splitQuerySQL = splitter.createDynamicSplitQuerySQL(split);
        Assertions.assertEquals(
                "SELECT * FROM (select * from table1) tmp WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitQuerySQL);
    }

    @Test
    public void testEfficientShardingThroughSampling() throws NoSuchMethodException {
        TablePath tablePath = new TablePath("db", "xe", "table");

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1}, 1000, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 10),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1}, 1000, 1),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3}, 1000, 3),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5}, 1000, 3),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5}, 1000, 2),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 1),
                Arrays.asList(DynamicChunkSplitter.ChunkRange.of(null, null)));

        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 3),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 4),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 5),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, null)));
        check(
                DynamicChunkSplitter.efficientShardingThroughSampling(
                        tablePath, new Object[] {1, 2, 3, 4, 5, 6}, 1000, 6),
                Arrays.asList(
                        DynamicChunkSplitter.ChunkRange.of(null, 1),
                        DynamicChunkSplitter.ChunkRange.of(1, 2),
                        DynamicChunkSplitter.ChunkRange.of(2, 3),
                        DynamicChunkSplitter.ChunkRange.of(3, 4),
                        DynamicChunkSplitter.ChunkRange.of(4, 5),
                        DynamicChunkSplitter.ChunkRange.of(5, 6),
                        DynamicChunkSplitter.ChunkRange.of(6, null)));
    }

    private void check(
            List<DynamicChunkSplitter.ChunkRange> a, List<DynamicChunkSplitter.ChunkRange> b) {
        checkRule(b);
        assertEquals(a, b);
    }

    private void checkRule(List<DynamicChunkSplitter.ChunkRange> a) {
        for (int i = 0; i < a.size(); i++) {
            if (i == 0) {
                assertNull(a.get(i).getChunkStart());
            }
            if (i == a.size() - 1) {
                assertNull(a.get(i).getChunkEnd());
            }
            // current chunk start should be equal to previous chunk end
            if (i > 0) {
                assertEquals(a.get(i - 1).getChunkEnd(), a.get(i).getChunkStart());
            }
            if (i > 0 && i < a.size() - 1) {
                // current chunk end should be greater than current chunk start
                assertTrue((int) a.get(i).getChunkEnd() > (int) a.get(i).getChunkStart());
            }
        }
    }
}
