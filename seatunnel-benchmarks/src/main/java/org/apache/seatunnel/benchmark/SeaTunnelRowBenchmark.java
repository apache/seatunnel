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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Benchmarks core {@link SeaTunnelRow} operations used in source, transform, and sink hot paths.
 */
public class SeaTunnelRowBenchmark extends BenchmarkBase {

    private static final String TRACE_PAYLOAD_OPTION_KEY = "__st_trace_payload";
    private static final int[] PROJECTION = new int[] {0, 1, 3, 5};

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {
                        "id", "name", "score", "enabled", "shard", "payload", "tags", "amount"
                    },
                    new SeaTunnelDataType<?>[] {
                        BasicType.LONG_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.DOUBLE_TYPE,
                        BasicType.BOOLEAN_TYPE,
                        BasicType.INT_TYPE,
                        PrimitiveByteArrayType.INSTANCE,
                        ArrayType.STRING_ARRAY_TYPE,
                        new DecimalType(20, 4)
                    });

    @Param({"1024"})
    private int rowCount;

    private SeaTunnelRow[] plainRows;
    private SeaTunnelRow[] optionRows;
    private SeaTunnelRow[] traceRows;
    private SeaTunnelRow[] cachedSizeRows;
    private int cursor;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + SeaTunnelRowBenchmark.class.getCanonicalName() + ".*")
                        .build();
        new Runner(options).run();
    }

    @Setup
    public void setUp() {
        plainRows = new SeaTunnelRow[rowCount];
        optionRows = new SeaTunnelRow[rowCount];
        traceRows = new SeaTunnelRow[rowCount];
        cachedSizeRows = new SeaTunnelRow[rowCount];
        for (int i = 0; i < rowCount; i++) {
            plainRows[i] = newRow(i);
            optionRows[i] = newRowWithOptions(i, false);
            traceRows[i] = newRowWithOptions(i, true);
            cachedSizeRows[i] = newRow(i);
            cachedSizeRows[i].getBytesSize(ROW_TYPE);
        }
        cursor = 0;
    }

    @Benchmark
    public SeaTunnelRow copyPlainRow() {
        return nextPlainRow().copy();
    }

    @Benchmark
    public SeaTunnelRow copyRowWithOptions() {
        return nextOptionRow().copy();
    }

    @Benchmark
    public SeaTunnelRow copyRowWithTracePayload() {
        return nextTraceRow().copy();
    }

    @Benchmark
    public SeaTunnelRow copyProjectedPlainRow() {
        return nextPlainRow().copy(PROJECTION);
    }

    @Benchmark
    public SeaTunnelRow copyProjectedRowWithOptions() {
        return nextOptionRow().copy(PROJECTION);
    }

    @Benchmark
    public int copyThenMutateCopiedOptions() {
        SeaTunnelRow copied = nextOptionRow().copy();
        copied.getOptions().put(TRACE_PAYLOAD_OPTION_KEY, new byte[] {1, 2, 3, 4});
        return copied.getOptions().size();
    }

    @Benchmark
    public long readFields() {
        SeaTunnelRow row = nextPlainRow();
        return ((Long) row.getField(0))
                + ((String) row.getField(1)).length()
                + Math.round((Double) row.getField(2))
                + (((Boolean) row.getField(3)) ? 1 : 0)
                + ((Integer) row.getField(4))
                + ((byte[]) row.getField(5)).length
                + ((String[]) row.getField(6)).length
                + ((BigDecimal) row.getField(7)).longValue();
    }

    @Benchmark
    public int getBytesSizeCached() {
        return nextCachedSizeRow().getBytesSize(ROW_TYPE);
    }

    @Benchmark
    public int createRowAndGetBytesSize() {
        SeaTunnelRow row = newRow(nextIndex());
        return row.getBytesSize(ROW_TYPE);
    }

    @Benchmark
    public int createRowWithSetField() {
        int id = nextIndex();
        SeaTunnelRow row = new SeaTunnelRow(ROW_TYPE.getTotalFields());
        row.setTableId("benchmark_table_" + (id % 16));
        row.setRowKind((id & 1) == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
        row.setField(0, (long) id);
        row.setField(1, "seatunnel-row-" + id);
        row.setField(2, id * 0.01D);
        row.setField(3, (id & 1) == 0);
        row.setField(4, id % 128);
        row.setField(5, payload(id));
        row.setField(6, tags(id));
        row.setField(7, amount(id));
        return row.getArity();
    }

    private static SeaTunnelRow newRow(int id) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            (long) id,
                            "seatunnel-row-" + id,
                            id * 0.01D,
                            (id & 1) == 0,
                            id % 128,
                            payload(id),
                            tags(id),
                            amount(id)
                        });
        row.setTableId("benchmark_table_" + (id % 16));
        row.setRowKind((id & 1) == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
        return row;
    }

    private static SeaTunnelRow newRowWithOptions(int id, boolean tracePayload) {
        SeaTunnelRow row = newRow(id);
        Map<String, Object> options = new HashMap<>();
        options.put("source", "benchmark");
        options.put("partition", id % 32);
        options.put("offset", (long) id * 100);
        if (tracePayload) {
            options.put(TRACE_PAYLOAD_OPTION_KEY, payload(id));
        }
        row.setOptions(options);
        return row;
    }

    private SeaTunnelRow nextPlainRow() {
        return plainRows[Math.floorMod(nextIndex(), plainRows.length)];
    }

    private SeaTunnelRow nextOptionRow() {
        return optionRows[Math.floorMod(nextIndex(), optionRows.length)];
    }

    private SeaTunnelRow nextTraceRow() {
        return traceRows[Math.floorMod(nextIndex(), traceRows.length)];
    }

    private SeaTunnelRow nextCachedSizeRow() {
        return cachedSizeRows[Math.floorMod(nextIndex(), cachedSizeRows.length)];
    }

    private int nextIndex() {
        return cursor++;
    }

    private static byte[] payload(int id) {
        return new byte[] {
            (byte) id,
            (byte) (id >>> 8),
            (byte) (id >>> 16),
            (byte) (id >>> 24),
            (byte) (id * 31),
            (byte) (id * 17),
            (byte) (id * 7),
            (byte) (id * 3)
        };
    }

    private static String[] tags(int id) {
        return new String[] {"tag-" + (id % 8), "bucket-" + (id % 64), "row-" + id};
    }

    private static BigDecimal amount(int id) {
        return BigDecimal.valueOf(id * 1000L + 1234L, 4);
    }
}
