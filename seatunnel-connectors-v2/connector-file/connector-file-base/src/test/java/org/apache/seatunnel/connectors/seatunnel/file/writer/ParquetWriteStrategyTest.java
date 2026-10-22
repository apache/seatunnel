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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class ParquetWriteStrategyTest {
    private static final String TMP_PATH = "file:///tmp/seatunnel/parquet/int96/test";

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testCloseWriterWhenRollingFile() throws Exception {
        String tmpPath = "file:///tmp/seatunnel/parquet/rolling/test-" + System.nanoTime();
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/rolling");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("batch_size", 1);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        ParquetWriteStrategy writeStrategy =
                new ParquetWriteStrategy(
                        new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), rowType));
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
        writeStrategy.init(hadoopConf, "rolling-test", "rolling-test", 0);
        writeStrategy.beginTransaction(1L);

        writeStrategy.write(new SeaTunnelRow(new Object[] {"first"}));
        writeStrategy.write(new SeaTunnelRow(new Object[] {"second"}));

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(tmpPath);
        Assertions.assertEquals(1, readFiles.size());
        String rolledFile = readFiles.get(0);
        Assertions.assertTrue(rolledFile.endsWith("_0.parquet"));
        readStrategy.getSeaTunnelRowTypeInfo(rolledFile);
        List<String> values = new ArrayList<>();
        readStrategy.read(rolledFile, "test", collectorForFirstField(values));
        Assertions.assertEquals(Arrays.asList("first"), values);

        writeStrategy.finishAndCloseFile();
        writeStrategy.close();
        Assertions.assertEquals(2, readStrategy.getFileNamesByPath(tmpPath).size());
        readStrategy.close();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteInt96() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("parquet_avro_write_timestamp_as_int96", "true");
        writeConfig.put("parquet_avro_write_fixed_as_int96", Arrays.asList("F3_Bytes"));

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text", "f2_timestamp", "F3_Bytes", "createTime"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(
                new SeaTunnelRow(
                        new Object[] {
                            "test", LocalDateTime.now(), new byte[12], LocalDateTime.now()
                        }));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(
                                new org.apache.hadoop.fs.Path(readFilePath),
                                new Configuration()))) {
            FileMetaData metadata = reader.getFooter().getFileMetaData();
            Type f1Type = metadata.getSchema().getType("f1_text");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    f1Type.asPrimitiveType().getPrimitiveTypeName());
            Assertions.assertEquals(
                    LogicalTypeAnnotation.stringType(), f1Type.getLogicalTypeAnnotation());

            Type f2Type = metadata.getSchema().getType("f2_timestamp");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    f2Type.asPrimitiveType().getPrimitiveTypeName());
            Type f3Type = metadata.getSchema().getType("f3_bytes");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    f3Type.asPrimitiveType().getPrimitiveTypeName());
            Type createTimeType = metadata.getSchema().getType("createtime");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    createTimeType.asPrimitiveType().getPrimitiveTypeName());
        }

        SeaTunnelRowType readRowType = readStrategy.getSeaTunnelRowTypeInfo(readFilePath);
        Assertions.assertEquals(
                BasicType.STRING_TYPE.getSqlType(), readRowType.getFieldType(0).getSqlType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE.getSqlType(),
                readRowType.getFieldType(1).getSqlType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE.getSqlType(),
                readRowType.getFieldType(2).getSqlType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE.getSqlType(),
                readRowType.getFieldType(3).getSqlType());
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertTrue(record.getField(0) instanceof String);
                        Assertions.assertTrue(record.getField(1) instanceof LocalDateTime);
                        Assertions.assertTrue(record.getField(2) instanceof LocalDateTime);
                        Assertions.assertTrue(record.getField(3) instanceof LocalDateTime);
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(readFilePath, "test", readCollector);
        Assertions.assertEquals(1, readRows.size());
        readStrategy.close();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteInt96WithMixedCaseTimestampColumn() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH + "-mixed-case");
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96-mixed-case");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("parquet_avro_write_timestamp_as_int96", "true");

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"createTime"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(new SeaTunnelRow(new Object[] {LocalDateTime.now()}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH + "-mixed-case");
        Assertions.assertEquals(1, readFiles.size());
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(
                                new org.apache.hadoop.fs.Path(readFiles.get(0)),
                                new Configuration()))) {
            FileMetaData metadata = reader.getFooter().getFileMetaData();
            Type createTimeType = metadata.getSchema().getType("createtime");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    createTimeType.asPrimitiveType().getPrimitiveTypeName());
        }
        readStrategy.close();
    }

    private static Collector<SeaTunnelRow> collectorForFirstField(List<String> values) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow record) {
                values.add((String) record.getField(0));
            }

            @Override
            public Object getCheckpointLock() {
                return null;
            }
        };
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteInt96IsUnaffectedByANonGregorianDefaultLocale() throws Exception {
        // INT96 is written by converting the value through java.util.Calendar. Calendar.getInstance
        // consults the default locale as well as the time zone, and the locale selects the calendar
        // system, so under a Thai default JVM locale it returns a BuddhistCalendar whose
        // get(Calendar.YEAR) is the Buddhist year. That value was previously used as a Gregorian
        // year, writing the timestamp 543 years in the future into the file with no error.
        //
        // The existing INT96 tests above cannot catch this: they assert the parquet schema types
        // and write LocalDateTime.now(), never comparing a round-tripped value.
        LocalDateTime value = LocalDateTime.of(2024, 3, 15, 13, 45, 30);
        Locale previous = Locale.getDefault();
        LocalDateTime underUs;
        LocalDateTime underTh;
        LocalDateTime underJa;
        try {
            Locale.setDefault(Locale.US);
            underUs = int96RoundTrip("locale-us", value);
            // Plain th_TH is enough; no -u-ca-buddhist extension is needed. This is what an OS
            // locale or -Duser.language=th -Duser.country=TH gives. BuddhistCalendar reports
            // get(Calendar.YEAR) as 2567 for 2024.
            Locale.setDefault(new Locale("th", "TH"));
            underTh = int96RoundTrip("locale-th", value);
            // A second, differently shaped calendar system: JapaneseImperialCalendar reports
            // get(Calendar.YEAR) as the era year, 6 for Reiwa 6, rather than an offset year.
            Locale.setDefault(Locale.forLanguageTag("ja-JP-u-ca-japanese"));
            underJa = int96RoundTrip("locale-ja", value);
        } finally {
            Locale.setDefault(previous);
        }

        Assertions.assertEquals(value, underUs, "the en_US round trip must be exact");
        Assertions.assertEquals(
                value,
                underTh,
                "the th_TH round trip must be exact; a Buddhist year written as a Gregorian one "
                        + "shifts the persisted timestamp by 543 years");
        Assertions.assertEquals(
                value,
                underJa,
                "the ja-JP-u-ca-japanese round trip must be exact; an imperial era year written "
                        + "as a Gregorian one moves the persisted timestamp to year 6");

        // The time of day travels through the same Calendar and is asserted in its own right,
        // not only as part of the value above.
        Assertions.assertEquals(value.toLocalTime(), underUs.toLocalTime(), "en_US time of day");
        Assertions.assertEquals(value.toLocalTime(), underTh.toLocalTime(), "th_TH time of day");
        Assertions.assertEquals(
                value.toLocalTime(), underJa.toLocalTime(), "ja-JP-u-ca-japanese time of day");
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteTimestampKeepsMicroseconds() throws Exception {
        // DATETIME(6) / TIMESTAMP(6) sources carry microseconds; INT64 TIMESTAMP_MILLIS dropped
        // them
        LocalDateTime[] values = {
            LocalDateTime.of(2026, 4, 27, 20, 50, 29, 6_733_000),
            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1_000),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_000),
            LocalDateTime.of(1900, 6, 15, 12, 30, 45, 123_456_000),
            LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000),
            LocalDateTime.of(2024, 3, 15, 13, 45, 30)
        };
        String tmpPath = writeTimestamps("micros", false, values);
        String file = parquetFile(tmpPath);

        PrimitiveType type;
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(
                                new org.apache.hadoop.fs.Path(file), new Configuration()))) {
            type =
                    reader.getFooter()
                            .getFileMetaData()
                            .getSchema()
                            .getType("f2_timestamp")
                            .asPrimitiveType();
        }
        Assertions.assertEquals(PrimitiveType.PrimitiveTypeName.INT64, type.getPrimitiveTypeName());
        // isAdjustedToUTC=true is what TIMESTAMP_MILLIS meant, so readers see the same type as
        // before
        Assertions.assertEquals(
                LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS),
                type.getLogicalTypeAnnotation());

        // raw stored values, checked without ParquetReadStrategy
        List<Long> raw = new ArrayList<>();
        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file))
                        .build()) {
            for (Group group = reader.read(); group != null; group = reader.read()) {
                raw.add(group.getLong("f2_timestamp", 0));
            }
        }
        List<Long> expectedRaw = new ArrayList<>();
        for (LocalDateTime value : values) {
            // floor to micros; ChronoUnit.MICROS.between overflows at year 9999 and truncates
            Instant instant = value.atZone(ZoneId.systemDefault()).toInstant();
            expectedRaw.add(instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000);
        }
        Assertions.assertEquals(expectedRaw, raw);

        Assertions.assertEquals(Arrays.asList(values), readTimestamps(tmpPath));
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteTimestampTruncatesBelowMicroseconds() throws Exception {
        String tmpPath =
                writeTimestamps(
                        "nanos", false, LocalDateTime.of(2026, 4, 27, 20, 50, 29, 6_733_999));
        Assertions.assertEquals(
                Arrays.asList(LocalDateTime.of(2026, 4, 27, 20, 50, 29, 6_733_000)),
                readTimestamps(tmpPath));
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteInt96KeepsSubMillisecondPrecision() throws Exception {
        LocalDateTime micros = LocalDateTime.of(2026, 4, 27, 20, 50, 29, 6_733_000);
        LocalDateTime nanos = LocalDateTime.of(2026, 4, 27, 20, 50, 29, 6_733_123);
        LocalDateTime preEpoch = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_000);
        Assertions.assertEquals(micros, int96RoundTrip("micros", micros));
        // INT96 stores nanoseconds of the day, so nothing below micros is lost either
        Assertions.assertEquals(nanos, int96RoundTrip("nanos", nanos));
        Assertions.assertEquals(preEpoch, int96RoundTrip("pre-epoch", preEpoch));
    }

    /** Writes one timestamp as INT96 and reads it back, returning the round-tripped value. */
    private static LocalDateTime int96RoundTrip(String tag, LocalDateTime value) throws Exception {
        List<LocalDateTime> read = readTimestamps(writeTimestamps(tag, true, value));
        Assertions.assertEquals(1, read.size());
        return read.get(0);
    }

    /** Writes the timestamps to one parquet file and returns its directory. */
    private static String writeTimestamps(String tag, boolean int96, LocalDateTime... values)
            throws Exception {
        String tmpPath = TMP_PATH + "-" + tag;
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96-" + tag);
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("parquet_avro_write_timestamp_as_int96", String.valueOf(int96));

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f2_timestamp"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, tag, tag, 0);
        writeStrategy.beginTransaction(1L);
        for (LocalDateTime value : values) {
            writeStrategy.write(new SeaTunnelRow(new Object[] {value}));
        }
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();
        return tmpPath;
    }

    /** Returns the single parquet file written under tmpPath. */
    private static String parquetFile(String tmpPath) throws Exception {
        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));
        List<String> files = readStrategy.getFileNamesByPath(tmpPath);
        readStrategy.close();
        Assertions.assertEquals(1, files.size());
        return files.get(0);
    }

    /** Reads the single parquet file under tmpPath with ParquetReadStrategy. */
    private static List<LocalDateTime> readTimestamps(String tmpPath) throws Exception {
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(tmpPath);
        Assertions.assertEquals(1, readFiles.size());
        readStrategy.getSeaTunnelRowTypeInfo(readFiles.get(0));
        List<SeaTunnelRow> readRows = new ArrayList<>();
        readStrategy.read(
                readFiles.get(0),
                "",
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                });
        readStrategy.close();
        List<LocalDateTime> result = new ArrayList<>();
        for (SeaTunnelRow row : readRows) {
            result.add((LocalDateTime) row.getField(0));
        }
        return result;
    }
}
