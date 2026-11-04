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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for HiveSink config generation. */
public class HiveSinkConfigTest {

    @TempDir java.nio.file.Path tempDirectory;

    @Test
    void testParquetCompressionFooterAndRows() throws Exception {
        for (String scenario : new String[] {"inherited", "override", "uncompressed", "gzip"}) {
            String tableCodec =
                    "uncompressed".equals(scenario)
                            ? "UNCOMPRESSED"
                            : "gzip".equals(scenario) ? "GZIP" : "SNAPPY";
            FileSinkConfig config =
                    parquetCompression(
                            true, "override".equals(scenario) ? "NONE" : null, tableCodec);
            CompressionCodecName expected =
                    "inherited".equals(scenario)
                            ? CompressionCodecName.SNAPPY
                            : "gzip".equals(scenario)
                                    ? CompressionCodecName.GZIP
                                    : CompressionCodecName.UNCOMPRESSED;
            java.nio.file.Path file = tempDirectory.resolve(scenario + ".parquet");
            Schema schema =
                    SchemaBuilder.record("test_row")
                            .fields()
                            .requiredString("id")
                            .optionalString("name")
                            .endRecord();
            try (ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(localOutputFile(file))
                            .withSchema(schema)
                            .withConf(new Configuration())
                            .withCompressionCodec(
                                    config.getCompressFormat().getParquetCompression())
                            .build()) {
                for (int id = 1; id <= 3; id++) {
                    GenericRecord row = new GenericData.Record(schema);
                    row.put("id", String.valueOf(id));
                    row.put("name", id == 2 ? null : "name-" + id);
                    writer.write(row);
                }
            }
            try (ParquetFileReader reader =
                    ParquetFileReader.open(
                            HadoopInputFile.fromPath(
                                    new Path(file.toUri()), new Configuration()))) {
                Assertions.assertFalse(reader.getFooter().getBlocks().isEmpty());
                long rows = 0;
                for (BlockMetaData block : reader.getFooter().getBlocks()) {
                    rows += block.getRowCount();
                    Assertions.assertFalse(block.getColumns().isEmpty());
                    for (ColumnChunkMetaData column : block.getColumns()) {
                        Assertions.assertEquals(expected, column.getCodec());
                    }
                }
                Assertions.assertEquals(3, rows);
            }
            try (ParquetReader<GenericRecord> reader =
                    AvroParquetReader.<GenericRecord>builder(new Path(file.toUri()))
                            .withConf(new Configuration())
                            .build()) {
                for (int id = 1; id <= 3; id++) {
                    GenericRecord row = reader.read();
                    Assertions.assertNotNull(row);
                    Assertions.assertEquals(String.valueOf(id), row.get("id").toString());
                    Assertions.assertEquals(
                            id == 2 ? null : "name-" + id,
                            row.get("name") == null ? null : row.get("name").toString());
                }
                Assertions.assertNull(reader.read());
            }
        }
    }

    // Avoid Hadoop's native permission helpers when writing local test files on Windows.
    private static OutputFile localOutputFile(java.nio.file.Path file) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                OutputStream stream =
                        Files.newOutputStream(file, java.nio.file.StandardOpenOption.CREATE_NEW);
                return new PositionOutputStream() {
                    private long position;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int value) throws IOException {
                        stream.write(value);
                        position++;
                    }

                    @Override
                    public void write(byte[] bytes, int offset, int length) throws IOException {
                        stream.write(bytes, offset, length);
                        position += length;
                    }

                    @Override
                    public void flush() throws IOException {
                        stream.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        stream.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    @Test
    void testTableCompressionDisabledByDefault() throws Exception {
        Assertions.assertEquals(
                CompressFormat.NONE,
                compressionConfig(null, null, "SNAPPY", true).getCompressFormat());
    }

    @Test
    void testTableCompressionExplicitlyDisabled() throws Exception {
        Assertions.assertEquals(
                CompressFormat.NONE,
                compressionConfig(false, null, "SNAPPY", true).getCompressFormat());
    }

    @Test
    void testInheritOrcCompression() throws Exception {
        Assertions.assertEquals(
                CompressFormat.SNAPPY,
                compressionConfig(true, null, " snappy ", true).getCompressFormat());
        for (CompressFormat codec : CompressFormat.values()) {
            if (codec == CompressFormat.NONE || codec.getOrcCompression() != CompressionKind.NONE) {
                Assertions.assertEquals(
                        codec,
                        compressionConfig(true, null, codec.name(), true).getCompressFormat());
            }
        }
    }

    @Test
    void testExplicitCompressionOverridesTable() throws Exception {
        Assertions.assertEquals(
                CompressFormat.NONE,
                compressionConfig(true, "NONE", "SNAPPY", true).getCompressFormat());
        Assertions.assertEquals(
                CompressFormat.ZLIB,
                compressionConfig(true, "ZLIB", "invalid", true).getCompressFormat());
    }

    @Test
    void testMissingOrBlankTableCompressionKeepsDefault() throws Exception {
        for (String codec : new String[] {null, "", "   "}) {
            Assertions.assertEquals(
                    CompressFormat.NONE,
                    compressionConfig(true, null, codec, true).getCompressFormat());
        }
    }

    @Test
    void testTableCompressionDoesNotApplyToOtherFormats() throws Exception {
        Assertions.assertEquals(
                CompressFormat.NONE,
                compressionConfig(true, null, "SNAPPY", false).getCompressFormat());
        Assertions.assertEquals(
                CompressFormat.NONE,
                compressionConfig(true, null, "invalid", FileFormat.PARQUET).getCompressFormat());
    }

    @Test
    void testInvalidOrcCompressionRejected() {
        for (String codec : new String[] {"invalid", "GZIP", "BROTLI"}) {
            HiveConnectorException error =
                    Assertions.assertThrows(
                            HiveConnectorException.class,
                            () -> compressionConfig(true, null, codec, true));
            Assertions.assertTrue(error.getMessage().contains("orc.compress"));
            Assertions.assertTrue(error.getMessage().contains("default.test_table"));
        }
        for (CompressFormat codec : CompressFormat.values()) {
            if (codec != CompressFormat.NONE && codec.getOrcCompression() == CompressionKind.NONE) {
                Assertions.assertThrows(
                        HiveConnectorException.class,
                        () -> compressionConfig(true, null, codec.name(), true));
            }
        }
    }

    private static FileSinkConfig compressionConfig(
            Boolean enabled, String explicitCodec, String tableCodec, boolean orc)
            throws Exception {
        return compressionConfig(
                enabled, explicitCodec, tableCodec, orc ? FileFormat.ORC : FileFormat.TEXT);
    }

    private static FileSinkConfig compressionConfig(
            Boolean enabled, String explicitCodec, String tableCodec, FileFormat format)
            throws Exception {
        Map<String, String> parameters = new HashMap<>();
        if (tableCodec != null) {
            parameters.put("orc.compress", tableCodec);
        }
        return compressionConfig(enabled, explicitCodec, format, parameters);
    }

    @Test
    void testInheritParquetCompression() throws Exception {
        for (String codec :
                new String[] {"SNAPPY", "GZIP", "ZSTD", "LZ4", "LZO", "BROTLI", "NONE"}) {
            Assertions.assertEquals(
                    CompressFormat.valueOf(codec),
                    parquetCompression(
                                    true,
                                    null,
                                    " " + codec.toLowerCase(java.util.Locale.ROOT) + " ")
                            .getCompressFormat());
        }
        Assertions.assertEquals(
                CompressFormat.NONE,
                parquetCompression(true, null, " uncompressed ").getCompressFormat());
    }

    @Test
    void testParquetCompressionPrecedenceAndDefault() throws Exception {
        Assertions.assertEquals(
                CompressFormat.NONE, parquetCompression(null, null, "SNAPPY").getCompressFormat());
        Assertions.assertEquals(
                CompressFormat.NONE,
                parquetCompression(false, null, "invalid").getCompressFormat());
        Assertions.assertEquals(
                CompressFormat.NONE,
                parquetCompression(true, "NONE", "SNAPPY").getCompressFormat());
        Assertions.assertEquals(
                CompressFormat.GZIP,
                parquetCompression(true, "GZIP", "invalid").getCompressFormat());
        for (String codec : new String[] {null, "", "   "}) {
            Assertions.assertEquals(
                    CompressFormat.NONE, parquetCompression(true, null, codec).getCompressFormat());
        }
        Assertions.assertEquals(
                CompressFormat.NONE,
                compressionConfig(true, null, FileFormat.PARQUET, null).getCompressFormat());
    }

    @Test
    void testInvalidParquetCompressionRejected() {
        for (String codec : new String[] {"invalid", "ZLIB"}) {
            HiveConnectorException error =
                    Assertions.assertThrows(
                            HiveConnectorException.class,
                            () -> parquetCompression(true, null, codec));
            Assertions.assertTrue(error.getMessage().contains("parquet.compression"));
            Assertions.assertTrue(error.getMessage().contains("default.test_table"));
        }
    }

    @Test
    void testParquetPropertyDoesNotApplyToOrcOrText() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("parquet.compression", "invalid");
        for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.TEXT}) {
            Assertions.assertEquals(
                    CompressFormat.NONE,
                    compressionConfig(true, null, format, parameters).getCompressFormat());
        }
    }

    private static FileSinkConfig parquetCompression(
            Boolean enabled, String explicitCodec, String tableCodec) throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("parquet.compression", tableCodec);
        return compressionConfig(enabled, explicitCodec, FileFormat.PARQUET, parameters);
    }

    private static FileSinkConfig compressionConfig(
            Boolean enabled,
            String explicitCodec,
            FileFormat format,
            Map<String, String> parameters)
            throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(HiveSinkOptions.TABLE_NAME.key(), "default.test_table");
        if (enabled != null) {
            options.put(HiveSinkOptions.USE_TABLE_COMPRESSION.key(), enabled);
        }
        if (explicitCodec != null) {
            options.put(FileBaseSinkOptions.COMPRESS_CODEC.key(), explicitCodec);
        }
        Table table =
                mockTextTable(
                        "default",
                        "test_table",
                        "file:/tmp/hive/test_table",
                        listOf(
                                new FieldSchema("id", "string", null),
                                new FieldSchema("name", "string", null)),
                        new ArrayList<>());
        if (format == FileFormat.ORC) {
            table.getSd().setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        } else if (format == FileFormat.PARQUET) {
            table.getSd()
                    .setInputFormat(
                            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        }
        table.setParameters(parameters);
        try (MockedStatic<HiveTableUtils> mocked = Mockito.mockStatic(HiveTableUtils.class)) {
            mocked.when(() -> HiveTableUtils.getTableInfo(Mockito.any())).thenReturn(table);
            mocked.when(() -> HiveTableUtils.parseFileFormat(Mockito.any(Table.class)))
                    .thenCallRealMethod();
            return extractFileSinkConfig(
                    new HiveSink(ReadonlyConfig.fromMap(options), buildCatalogTable()));
        }
    }

    @Test
    void testDefaultFileNameExpressionAppliedWhenAbsent() throws Exception {
        // Build minimal input config without file_name_expression
        Map<String, Object> options = new HashMap<>();
        options.put(HiveSinkOptions.TABLE_NAME.key(), "default.test_table");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(options);

        // Mock Hive table metadata and file format
        try (MockedStatic<HiveTableUtils> mockedStatic = Mockito.mockStatic(HiveTableUtils.class)) {
            Table table =
                    mockTextTable(
                            "default",
                            "test_table",
                            "file:/tmp/hive/test_table",
                            listOf(
                                    new FieldSchema("id", "string", null),
                                    new FieldSchema("name", "string", null)),
                            new ArrayList<>());
            mockedStatic.when(() -> HiveTableUtils.getTableInfo(Mockito.any())).thenReturn(table);
            mockedStatic
                    .when(() -> HiveTableUtils.parseFileFormat(Mockito.any(Table.class)))
                    .thenCallRealMethod(); // inputFormat set in table, real method will return TEXT

            CatalogTable catalogTable = buildCatalogTable();
            HiveSink hiveSink = new HiveSink(readonlyConfig, catalogTable);
            FileSinkConfig cfg = extractFileSinkConfig(hiveSink);
            Assertions.assertEquals(
                    FileBaseSinkOptions.DEFAULT_FILE_NAME_EXPRESSION,
                    cfg.getFileNameExpression(),
                    "Should apply default ${transactionId} when user didn't configure file_name_expression");
        }
    }

    @Test
    void testRespectUserProvidedFileNameExpression() throws Exception {
        // Provide custom file_name_expression and disable transaction to pass validation
        Map<String, Object> options = new HashMap<>();
        options.put(HiveSinkOptions.TABLE_NAME.key(), "default.test_table");
        options.put(FileBaseSinkOptions.FILE_NAME_EXPRESSION.key(), "orders_${uuid}");
        options.put(FileBaseSinkOptions.IS_ENABLE_TRANSACTION.key(), false);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(options);

        try (MockedStatic<HiveTableUtils> mockedStatic = Mockito.mockStatic(HiveTableUtils.class)) {
            Table table =
                    mockTextTable(
                            "default",
                            "test_table",
                            "file:/tmp/hive/test_table",
                            listOf(new FieldSchema("id", "string", null)),
                            new ArrayList<>());
            mockedStatic.when(() -> HiveTableUtils.getTableInfo(Mockito.any())).thenReturn(table);
            mockedStatic
                    .when(() -> HiveTableUtils.parseFileFormat(Mockito.any(Table.class)))
                    .thenCallRealMethod();

            CatalogTable catalogTable = buildCatalogTable();
            HiveSink hiveSink = new HiveSink(readonlyConfig, catalogTable);
            FileSinkConfig cfg = extractFileSinkConfig(hiveSink);
            Assertions.assertEquals(
                    "orders_${uuid}",
                    cfg.getFileNameExpression(),
                    "HiveSink should not override user-provided file_name_expression");
        }
    }

    private static CatalogTable buildCatalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.STRING_TYPE, 100L, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 100L, true, null, null))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("test_catalog", "default", "test_table"),
                schema,
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }

    private static FileSinkConfig extractFileSinkConfig(HiveSink hiveSink) throws Exception {
        Field f = HiveSink.class.getDeclaredField("fileSinkConfig");
        f.setAccessible(true);
        return (FileSinkConfig) f.get(hiveSink);
    }

    private static List<FieldSchema> listOf(FieldSchema... fs) {
        List<FieldSchema> l = new ArrayList<>();
        for (FieldSchema f : fs) {
            l.add(f);
        }
        return l;
    }

    private static Table mockTextTable(
            String db,
            String tableName,
            String location,
            List<FieldSchema> cols,
            List<FieldSchema> partitions) {
        Table t = new Table();
        t.setDbName(db);
        t.setTableName(tableName);

        SerDeInfo serDeInfo = new SerDeInfo();
        Map<String, String> params = new HashMap<>();
        params.put("field.delim", ",");
        params.put("line.delim", "\n");
        serDeInfo.setParameters(params);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setSerdeInfo(serDeInfo);
        sd.setCols(cols);
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setLocation(location);
        t.setSd(sd);
        t.setPartitionKeys(partitions);
        return t;
    }
}
