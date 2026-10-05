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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ParquetWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/**
 * Asserts that {@link ParquetWriteStrategy} accepts the canonical Java type for every SeaTunnel
 * SqlType AND defensively coerces the most likely "looks-wrong" types Debezium may emit. The
 * canonical regression case is BOOLEAN ← Byte, which crashed v21 in stage when MySQL {@code
 * tinyint(1)} columns flowed through the Parquet sink as raw Byte values.
 */
@Slf4j
public class ParquetTypeCoercionTest {

    private static final String BASE_PATH = "file:///tmp/seatunnel/parquet/coercion";

    /**
     * v21 stage regression. MySQL {@code tinyint(1)} arrives as Byte but the SeaTunnel schema says
     * BOOLEAN; Avro requires Boolean. Without coercion in resolveObject, this throws {@code
     * java.lang.Byte cannot be cast to java.lang.Boolean} at AvroWriteSupport.
     */
    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testBooleanColumnAcceptsByteFromCdcTinyintOne() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "is_active"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE});

        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1, (byte) 1});
        SeaTunnelRow row0 = new SeaTunnelRow(new Object[] {2, (byte) 0});
        SeaTunnelRow rowBoolean = new SeaTunnelRow(new Object[] {3, Boolean.TRUE});

        List<Object[]> readBack =
                writeAndReadBack(
                        "boolean-from-byte",
                        rowType,
                        java.util.Arrays.asList(row1, row0, rowBoolean));

        Assertions.assertEquals(3, readBack.size());
        // After coercion every BOOLEAN cell must be a Boolean — never a Byte.
        for (Object[] r : readBack) {
            Assertions.assertTrue(
                    r[1] instanceof Boolean,
                    "BOOLEAN column read back as " + r[1].getClass() + " (value=" + r[1] + ")");
        }
        // Byte 1 → true, Byte 0 → false, Boolean.TRUE → true.
        Assertions.assertEquals(Boolean.TRUE, readBack.get(0)[1]);
        Assertions.assertEquals(Boolean.FALSE, readBack.get(1)[1]);
        Assertions.assertEquals(Boolean.TRUE, readBack.get(2)[1]);
    }

    /**
     * Type fidelity for the most common types we see in production (settlement / bookkeeping
     * pipelines): INT, BIGINT, SMALLINT, TINYINT, FLOAT, DOUBLE, DECIMAL, STRING, DATE, TIMESTAMP,
     * BYTES. Each column gets the canonical Java type — this test guards against regressions in
     * {@code resolveObject}'s case statements.
     */
    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testCanonicalTypesRoundTripFaithfully() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "c_tinyint",
                            "c_smallint",
                            "c_int",
                            "c_bigint",
                            "c_float",
                            "c_double",
                            "c_decimal",
                            "c_string",
                            "c_date",
                            "c_timestamp",
                            "c_bytes"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(20, 6),
                            BasicType.STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                        });

        LocalDate testDate = LocalDate.of(2026, 4, 26);
        LocalDateTime testTimestamp = LocalDateTime.of(2026, 4, 26, 14, 30, 15);
        BigDecimal testDecimal = new BigDecimal("12345678901234.567890");

        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            (byte) 7,
                            (short) 1234,
                            42,
                            9_999_999_999L,
                            3.14f,
                            2.71828,
                            testDecimal,
                            "hello",
                            testDate,
                            testTimestamp,
                            new byte[] {1, 2, 3, 4}
                        });

        List<Object[]> readBack =
                writeAndReadBack("canonical-types", rowType, java.util.Arrays.asList(row));

        Assertions.assertEquals(1, readBack.size());
        Object[] r = readBack.get(0);
        Assertions.assertEquals((byte) 7, ((Number) r[0]).byteValue());
        Assertions.assertEquals((short) 1234, ((Number) r[1]).shortValue());
        Assertions.assertEquals(42, ((Number) r[2]).intValue());
        Assertions.assertEquals(9_999_999_999L, ((Number) r[3]).longValue());
        Assertions.assertEquals(3.14f, ((Number) r[4]).floatValue(), 0.0001f);
        Assertions.assertEquals(2.71828, ((Number) r[5]).doubleValue(), 0.0000001);
        Assertions.assertEquals(0, testDecimal.compareTo((BigDecimal) r[6]));
        Assertions.assertEquals("hello", r[7]);
        Assertions.assertEquals(testDate, r[8]);
        Assertions.assertEquals(testTimestamp, r[9]);
        Assertions.assertArrayEquals(new byte[] {1, 2, 3, 4}, (byte[]) r[10]);
    }

    /**
     * Null values must round-trip as null for every nullable column type — otherwise downstream
     * consumers see surprising defaults or NPE.
     */
    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testNullValuesPreserved() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "amount", "is_active"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            new DecimalType(10, 2),
                            BasicType.BOOLEAN_TYPE
                        });

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, null, null, null});

        List<Object[]> readBack = writeAndReadBack("nulls", rowType, java.util.Arrays.asList(row));

        Assertions.assertEquals(1, readBack.size());
        Object[] r = readBack.get(0);
        Assertions.assertEquals(1, ((Number) r[0]).intValue());
        Assertions.assertNull(r[1]);
        Assertions.assertNull(r[2]);
        Assertions.assertNull(r[3]);
    }

    // ── helper ─────────────────────────────────────────────────────────────────

    private static List<Object[]> writeAndReadBack(
            String testName, SeaTunnelRowType rowType, List<SeaTunnelRow> rows) throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        String tmpPath = BASE_PATH + "/" + testName + "/tmp";
        String finalPath = BASE_PATH + "/" + testName + "/out";
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", finalPath);
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());

        FileSinkConfig sinkConfig =
                new FileSinkConfig(
                        ReadonlyConfig.fromConfig(ConfigFactory.parseMap(writeConfig)), rowType);
        ParquetWriteStrategy strategy = new ParquetWriteStrategy(sinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        strategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, testName, rowType));
        strategy.init(hadoopConf, "test-" + testName, "test-" + testName, 0);
        strategy.beginTransaction(1L);
        for (SeaTunnelRow row : rows) {
            strategy.write(row);
        }
        strategy.finishAndCloseFile();
        strategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(tmpPath);
        Assertions.assertFalse(readFiles.isEmpty(), "no Parquet files produced under " + tmpPath);

        List<Object[]> readBack = new ArrayList<>();
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Object[] fields = new Object[record.getArity()];
                        for (int i = 0; i < record.getArity(); i++) {
                            fields[i] = record.getField(i);
                        }
                        readBack.add(fields);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        for (String file : readFiles) {
            // Must call getSeaTunnelRowTypeInfo() before read() — it initializes per-file state.
            readStrategy.getSeaTunnelRowTypeInfo(file);
            readStrategy.read(file, testName, collector);
        }
        readStrategy.close();
        return readBack;
    }
}
