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

package org.apache.seatunnel.e2e.connector.fluss;

import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypes;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

@Slf4j
public class FlussSourceIT extends FlussTestBase {

    private static final String DB_NAME = "fluss_source_db";
    private static final String TABLE_NAME = "fluss_source_tb";
    private static final String ALL_TYPES_TABLE = "fluss_all_types_tb";

    private static final String STREAM_DB = "fluss_stream_db";
    private static final String STREAM_SRC_TABLE = "fluss_stream_src";
    private static final String STREAM_SINK_TABLE = "fluss_stream_sink";

    private static final String LATEST_DB = "fluss_latest_db";
    private static final String LATEST_SRC_TABLE = "fluss_latest_src";
    private static final String LATEST_SINK_TABLE = "fluss_latest_sink";

    private static final Object[][] ROWS = {
        {1, "Alice", 20, 98.5d, true},
        {2, "Bob", 31, 88.0d, false},
        {3, "Carol", 42, 77.5d, true},
    };

    // ---------------------------------------------------------------------------------------------
    // BATCH: multi-row read, count + per-column type + value-range validation.
    // ---------------------------------------------------------------------------------------------
    @TestTemplate
    public void testFlussSource(TestContainer container) throws Exception {
        createDb(flussConnection, DB_NAME);
        createTable(flussConnection, DB_NAME, TABLE_NAME, getSourceSchema());
        writeRows(DB_NAME, TABLE_NAME);

        Container.ExecResult execResult = container.executeJob("/fluss_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    // ---------------------------------------------------------------------------------------------
    // BATCH: single row across the full type matrix, exact per-column value validation.
    // ---------------------------------------------------------------------------------------------
    @TestTemplate
    public void testFlussSourceAllTypes(TestContainer container) throws Exception {
        createDb(flussConnection, DB_NAME);
        createTable(flussConnection, DB_NAME, ALL_TYPES_TABLE, getAllTypesSchema());
        writeAllTypesRow(DB_NAME, ALL_TYPES_TABLE);

        Container.ExecResult execResult = container.executeJob("/fluss_all_types_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    // ---------------------------------------------------------------------------------------------
    // STREAMING: unbounded read must keep discovering rows appended after the job starts.
    // Zeta-only: the async run + status/cancel dance uses Zeta-only container APIs (getJobStatus /
    // cancelJob), so Flink/Spark are excluded permanently (their streaming path is exercised
    // elsewhere).
    // ---------------------------------------------------------------------------------------------
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Uses Zeta-only job status / cancel APIs")
    public void testFlussSourceStreaming(TestContainer container) throws Exception {
        createDb(flussConnection, STREAM_DB);
        createTable(flussConnection, STREAM_DB, STREAM_SRC_TABLE, getStreamSchema());
        createTable(flussConnection, STREAM_DB, STREAM_SINK_TABLE, getStreamSchema());

        // Rows written before the job starts: an earliest streaming read must pick them up.
        appendStreamRows(STREAM_SRC_TABLE, new Object[][] {{1, "a"}, {2, "b"}});

        // Zeta's REST API parses the job id as a long, so it must be numeric.
        String jobId = "96481357024680";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/fluss_streaming_to_fluss.conf", jobId);
                    } catch (Exception e) {
                        log.error("Streaming job execution failed", e);
                        throw new RuntimeException(e);
                    }
                });

        awaitJobRunning(container, jobId);

        // The two pre-start rows must arrive in the sink.
        await().atMost(3, TimeUnit.MINUTES)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        new HashSet<>(Arrays.asList("a", "b")),
                                        readSinkNames(STREAM_DB, STREAM_SINK_TABLE)));

        // Rows appended while the job is running must be discovered by the unbounded read.
        appendStreamRows(STREAM_SRC_TABLE, new Object[][] {{3, "c"}, {4, "d"}, {5, "e"}});

        await().atMost(3, TimeUnit.MINUTES)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        new HashSet<>(Arrays.asList("a", "b", "c", "d", "e")),
                                        readSinkNames(STREAM_DB, STREAM_SINK_TABLE)));

        container.cancelJob(jobId);
    }

    // ---------------------------------------------------------------------------------------------
    // STREAMING + start_mode=latest: only rows appended AFTER the job starts must be read; rows
    // written before the job starts must be skipped. Zeta-only for the same reason as the earliest
    // streaming test (async run + Zeta-only job status / cancel APIs).
    // ---------------------------------------------------------------------------------------------
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Uses Zeta-only job status / cancel APIs")
    public void testFlussSourceStreamingLatest(TestContainer container) throws Exception {
        createDb(flussConnection, LATEST_DB);
        createTable(flussConnection, LATEST_DB, LATEST_SRC_TABLE, getStreamSchema());
        createTable(flussConnection, LATEST_DB, LATEST_SINK_TABLE, getStreamSchema());

        // Rows written before the job starts: a latest streaming read must NOT pick them up.
        appendStreamRows(LATEST_DB, LATEST_SRC_TABLE, new Object[][] {{1, "old1"}, {2, "old2"}});

        // Zeta's REST API parses the job id as a long, so it must be numeric.
        String jobId = "96481357024681";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/fluss_streaming_latest_to_fluss.conf", jobId);
                    } catch (Exception e) {
                        log.error("Streaming (latest) job execution failed", e);
                        throw new RuntimeException(e);
                    }
                });

        awaitJobRunning(container, jobId);

        // The enumerator captures the bucket tail inside run(), which can lag behind the job
        // reaching RUNNING. Appending the asserted rows before that capture would place them
        // at/behind the captured latest offset and skip them (flaky on slow machines). So first
        // drive unique sync rows until one surfaces in the sink: that proves the reader is live and
        // has advanced past the pre-start tail, after which any appended row is strictly newer.
        AtomicInteger syncSeq = new AtomicInteger();
        await().atMost(2, TimeUnit.MINUTES)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            int id = 100 + syncSeq.incrementAndGet();
                            appendStreamRows(
                                    LATEST_DB,
                                    LATEST_SRC_TABLE,
                                    new Object[][] {{id, "sync" + id}});
                            Assertions.assertTrue(
                                    readSinkNames(LATEST_DB, LATEST_SINK_TABLE).stream()
                                            .anyMatch(name -> name.startsWith("sync")),
                                    "Reader is not yet live past the pre-start tail");
                        });

        // The reader is live now: rows appended here are strictly after the captured latest offset.
        appendStreamRows(
                LATEST_DB,
                LATEST_SRC_TABLE,
                new Object[][] {{3, "new3"}, {4, "new4"}, {5, "new5"}});

        await().atMost(3, TimeUnit.MINUTES)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        readSinkNames(LATEST_DB, LATEST_SINK_TABLE)
                                                .containsAll(Arrays.asList("new3", "new4", "new5")),
                                        "All post-start rows must be read"));

        // latest semantics: the two pre-start rows must never be read.
        Set<String> sinkNames = readSinkNames(LATEST_DB, LATEST_SINK_TABLE);
        Assertions.assertFalse(
                sinkNames.contains("old1") || sinkNames.contains("old2"),
                "Pre-start rows must be skipped by latest, but sink was: " + sinkNames);

        container.cancelJob(jobId);
    }

    // ---------------------------------------------------------------------------------------------
    // Schemas
    // ---------------------------------------------------------------------------------------------
    private Schema getSourceSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("score", DataTypes.DOUBLE())
                .column("active", DataTypes.BOOLEAN())
                .build();
    }

    private Schema getAllTypesSchema() {
        return Schema.newBuilder()
                .column("fbytes", DataTypes.BYTES())
                .column("fboolean", DataTypes.BOOLEAN())
                .column("fint", DataTypes.INT())
                .column("ftinyint", DataTypes.TINYINT())
                .column("fsmallint", DataTypes.SMALLINT())
                .column("fbigint", DataTypes.BIGINT())
                .column("ffloat", DataTypes.FLOAT())
                .column("fdouble", DataTypes.DOUBLE())
                .column("fdecimal", DataTypes.DECIMAL(30, 8))
                .column("fstring", DataTypes.STRING())
                .column("fdate", DataTypes.DATE())
                .column("ftime", DataTypes.TIME())
                .column("ftimestamp", DataTypes.TIMESTAMP())
                .column("ftimestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                .build();
    }

    private Schema getStreamSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .build();
    }

    // ---------------------------------------------------------------------------------------------
    // Writers
    // ---------------------------------------------------------------------------------------------
    private void writeRows(String dbName, String tableName) throws Exception {
        Table table = flussConnection.getTable(TablePath.of(dbName, tableName));
        try {
            AppendWriter writer = table.newAppend().createWriter();
            for (Object[] row : ROWS) {
                GenericRow genericRow = new GenericRow(row.length);
                genericRow.setField(0, row[0]);
                genericRow.setField(1, BinaryString.fromString((String) row[1]));
                genericRow.setField(2, row[2]);
                genericRow.setField(3, row[3]);
                genericRow.setField(4, row[4]);
                writer.append(genericRow);
            }
            writer.flush();
        } finally {
            table.close();
        }
        log.info("Wrote {} rows into {}.{}", ROWS.length, dbName, tableName);
    }

    /**
     * Writes one row exercising every Fluss data type the source converter supports. The internal
     * representations mirror {@code FlussSinkWriter#convert}; the values are asserted back in
     * {@code fluss_all_types_to_assert.conf}.
     */
    private void writeAllTypesRow(String dbName, String tableName) throws Exception {
        Table table = flussConnection.getTable(TablePath.of(dbName, tableName));
        try {
            AppendWriter writer = table.newAppend().createWriter();
            GenericRow row = new GenericRow(14);
            row.setField(0, new byte[] {1, 2, 3}); // fbytes -> base64 "AQID"
            row.setField(1, true); // fboolean
            row.setField(2, 42); // fint
            row.setField(3, (byte) 7); // ftinyint
            row.setField(4, (short) 1000); // fsmallint
            row.setField(5, 9000000000L); // fbigint
            row.setField(6, 1.5f); // ffloat
            row.setField(7, 2.5d); // fdouble
            row.setField(8, Decimal.fromBigDecimal(new BigDecimal("123.45000000"), 30, 8));
            row.setField(9, BinaryString.fromString("hello")); // fstring
            row.setField(10, (int) LocalDate.parse("2025-01-15").toEpochDay()); // fdate
            row.setField(
                    11,
                    (int)
                            (LocalTime.parse("12:34:56").toNanoOfDay()
                                    / 1_000_000)); // ftime (millis)
            row.setField(
                    12, TimestampNtz.fromLocalDateTime(LocalDateTime.parse("2025-01-15T12:34:56")));
            row.setField(13, TimestampLtz.fromInstant(Instant.parse("2025-01-15T04:34:56Z")));
            writer.append(row);
            writer.flush();
        } finally {
            table.close();
        }
        log.info("Wrote one all-types row into {}.{}", dbName, tableName);
    }

    private void appendStreamRows(String tableName, Object[][] rows) throws Exception {
        appendStreamRows(STREAM_DB, tableName, rows);
    }

    private void appendStreamRows(String dbName, String tableName, Object[][] rows)
            throws Exception {
        Table table = flussConnection.getTable(TablePath.of(dbName, tableName));
        try {
            AppendWriter writer = table.newAppend().createWriter();
            for (Object[] row : rows) {
                GenericRow genericRow = new GenericRow(2);
                genericRow.setField(0, row[0]);
                genericRow.setField(1, BinaryString.fromString((String) row[1]));
                writer.append(genericRow);
            }
            writer.flush();
        } finally {
            table.close();
        }
        log.info("Appended {} rows into {}.{}", rows.length, dbName, tableName);
    }

    // ---------------------------------------------------------------------------------------------
    // Read-back helpers
    // ---------------------------------------------------------------------------------------------
    /** Reads the {@code name} column of every row currently in the given log table. */
    private Set<String> readSinkNames(String dbName, String tableName) {
        Set<String> names = new HashSet<>();
        Table table = flussConnection.getTable(TablePath.of(dbName, tableName));
        LogScanner logScanner = table.newScan().createLogScanner();
        try {
            int numBuckets = table.getTableInfo().getNumBuckets();
            for (int i = 0; i < numBuckets; i++) {
                logScanner.subscribeFromBeginning(i);
            }
            // Poll a few times; two consecutive empty polls means the log tail is drained.
            int emptyRounds = 0;
            int seen = 0;
            while (emptyRounds < 2 && seen < 1000) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                if (scanRecords.isEmpty()) {
                    emptyRounds++;
                    continue;
                }
                emptyRounds = 0;
                for (TableBucket bucket : scanRecords.buckets()) {
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        InternalRow internalRow = record.getRow();
                        names.add(internalRow.getString(1).toString());
                        seen++;
                    }
                }
            }
        } finally {
            try {
                logScanner.close();
            } catch (Exception e) {
                log.warn("Failed to close log scanner", e);
            }
            try {
                table.close();
            } catch (Exception e) {
                log.warn("Failed to close table", e);
            }
        }
        return names;
    }

    private void awaitJobRunning(TestContainer container, String jobId) {
        await().atMost(2, TimeUnit.MINUTES)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals("RUNNING", container.getJobStatus(jobId)));
    }
}
