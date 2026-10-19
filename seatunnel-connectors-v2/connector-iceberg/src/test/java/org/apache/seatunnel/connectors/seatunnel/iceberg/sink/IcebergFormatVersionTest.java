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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.shade.com.google.common.collect.Iterables;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalog;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeWrapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergRecord;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergWriterFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.reader.IcebergFileScanTaskReader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.HadoopStreamFileIO;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IcebergFormatVersionTest {

    private static final TableIdentifier TABLE = TableIdentifier.of("db", "events");

    private static final LocalDateTime TS_1 = LocalDateTime.parse("2024-03-10T10:00:00.123456789");
    private static final LocalDateTime TS_2 = LocalDateTime.parse("2024-03-11T23:59:59.999999999");
    private static final OffsetDateTime TSTZ_1 =
            OffsetDateTime.parse("2024-03-10T10:00:00.123456789+08:00");
    private static final OffsetDateTime TSTZ_2 =
            OffsetDateTime.parse("2024-03-11T00:00:00.000000001Z");

    private InMemoryCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    @Test
    void autoCreatesFormatVersion3Table() {
        Table table =
                SchemaUtils.autoCreateTable(
                        catalog, TABLE, sinkConfig(formatVersion("3")), idAndName());

        assertEquals(3, formatVersion(table));
    }

    @Test
    void keepsFormatVersion2AsDefault() {
        Table table =
                SchemaUtils.autoCreateTable(
                        catalog, TABLE, sinkConfig(Collections.emptyMap()), idAndName());

        assertEquals(2, formatVersion(table));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "4", "three", " 3"})
    void rejectsUnsupportedFormatVersion(String formatVersion) {
        IcebergConnectorException exception =
                assertThrows(
                        IcebergConnectorException.class,
                        () ->
                                SchemaUtils.autoCreateTable(
                                        catalog,
                                        TABLE,
                                        sinkConfig(formatVersion(formatVersion)),
                                        idAndName()));

        assertEquals(
                IcebergConnectorErrorCode.UNSUPPORTED_TABLE_FORMAT_VERSION,
                exception.getSeaTunnelErrorCode());
        assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "Unsupported Iceberg table format-version '" + formatVersion + "'"),
                exception.getMessage());
        assertFalse(catalog.tableExists(TABLE));
    }

    @Test
    void rejectsUnsupportedFormatVersionThroughCatalog() {
        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), Collections.emptyMap());
        config.put(IcebergSinkOptions.AUTO_CREATE_PROPS.key(), formatVersion("4"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "test", "db", "events"),
                        idAndName(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        assertThrows(
                IcebergConnectorException.class,
                () ->
                        SchemaUtils.autoCreateTable(
                                catalog,
                                TablePath.of("db", "events"),
                                catalogTable,
                                ReadonlyConfig.fromMap(config)));
        assertFalse(catalog.tableExists(TABLE));
    }

    @Test
    void createsNanosecondTimestampsOnlyForFormatVersion3() {
        Table v3 =
                SchemaUtils.autoCreateTable(
                        catalog, TABLE, sinkConfig(formatVersion("3")), timestampSchema());
        assertEquals(Types.TimestampNanoType.withoutZone(), v3.schema().findType("ts_nanos"));
        assertEquals(Types.TimestampNanoType.withZone(), v3.schema().findType("tstz_nanos"));
        assertEquals(Types.TimestampType.withoutZone(), v3.schema().findType("ts_micros"));
        assertEquals(Types.TimestampType.withoutZone(), v3.schema().findType("ts_default"));

        TableIdentifier v2Identifier = TableIdentifier.of("db", "events_v2");
        Table v2 =
                SchemaUtils.autoCreateTable(
                        catalog, v2Identifier, sinkConfig(formatVersion("2")), timestampSchema());
        assertEquals(Types.TimestampType.withoutZone(), v2.schema().findType("ts_nanos"));
        assertEquals(Types.TimestampType.withZone(), v2.schema().findType("tstz_nanos"));
    }

    @ParameterizedTest
    @CsvSource({
        "parquet, day(ts_nanos)",
        "orc, day(ts_nanos)",
        "avro, day(ts_nanos)",
        "parquet, ts_nanos",
        "orc, tstz_nanos",
        "avro, ts_nanos"
    })
    void writesAndReadsNanosecondTimestamps(String fileFormat, String partitionKey)
            throws Exception {
        Map<String, String> props = new HashMap<>(formatVersion("3"));
        props.put("write.format.default", fileFormat);
        IcebergSinkConfig config = sinkConfig(props, partitionKey);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, timestampSchema());
        assertFalse(table.spec().isUnpartitioned());

        SeaTunnelRowType rowType = timestampRowType();
        List<WriteResult> results =
                write(
                        table,
                        config,
                        rowType,
                        row(1L, TS_1, TSTZ_1, RowKind.INSERT),
                        row(2L, TS_2, TSTZ_2, RowKind.INSERT));
        commit(table, results);

        // the source path: table schema -> SeaTunnel columns -> deserialized rows
        CatalogTable catalogTable = toCatalogTable(table);
        Column tsColumn = column(catalogTable, "ts_nanos");
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, tsColumn.getDataType());
        assertEquals(9, tsColumn.getScale());
        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                column(catalogTable, "tstz_nanos").getDataType());
        assertEquals(null, column(catalogTable, "ts_micros").getScale());

        Map<Long, SeaTunnelRow> rows = readAll(table, catalogTable);
        assertEquals(2, rows.size());
        assertEquals(TS_1, rows.get(1L).getField(1));
        assertEquals(TSTZ_1.toInstant(), ((OffsetDateTime) rows.get(1L).getField(2)).toInstant());
        assertEquals(TS_2, rows.get(2L).getField(1));
        assertEquals(TSTZ_2.toInstant(), ((OffsetDateTime) rows.get(2L).getField(2)).toInstant());

        // the read schema creates the same nanosecond columns again
        Table copy =
                SchemaUtils.autoCreateTable(
                        catalog,
                        TableIdentifier.of("db", "events_copy"),
                        sinkConfig(formatVersion("3")),
                        catalogTable.getTableSchema());
        assertEquals(table.schema().asStruct(), copy.schema().asStruct());
    }

    @Test
    void readsUnknownColumnAsNull() throws Exception {
        Table table =
                catalog.buildTable(
                                TABLE,
                                new org.apache.iceberg.Schema(
                                        Types.NestedField.required(1, "id", Types.LongType.get()),
                                        Types.NestedField.optional(
                                                2, "nothing", Types.UnknownType.get())))
                        .withProperty("format-version", "3")
                        .create();
        IcebergSinkConfig config = sinkConfig(Collections.emptyMap());
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.LONG_TYPE
                        });
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {7L});
        commit(table, write(table, config, rowType, row));

        CatalogTable catalogTable = toCatalogTable(table);
        assertEquals(BasicType.VOID_TYPE, column(catalogTable, "nothing").getDataType());
        Map<Long, SeaTunnelRow> rows = readAll(table, catalogTable);
        assertEquals(1, rows.size());
        assertEquals(null, rows.get(7L).getField(1));
    }

    @Test
    void readsRowsDeletedByDeletionVector() throws Exception {
        IcebergSinkConfig config = sinkConfig(formatVersion("3"));
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        List<WriteResult> results =
                write(
                        table,
                        config,
                        rowType,
                        new SeaTunnelRow(new Object[] {1L, "a"}),
                        new SeaTunnelRow(new Object[] {2L, "b"}),
                        new SeaTunnelRow(new Object[] {3L, "c"}));
        commit(table, results);

        // the way other engines delete rows in a format version 3 table
        DataFile dataFile = results.get(0).getDataFiles().get(0);
        OutputFileFactory fileFactory =
                OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
        DVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, path -> null);
        try (DVFileWriter closeable = dvWriter) {
            closeable.delete(dataFile.location(), 1L, table.spec(), null);
        }
        RowDelta rowDelta = table.newRowDelta();
        dvWriter.result().deleteFiles().forEach(rowDelta::addDeletes);
        rowDelta.commit();

        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(2, rows.size());
        assertFalse(rows.containsKey(2L));
    }

    @Test
    void readsFormatVersion3TableWithoutUnsupportedColumns() throws Exception {
        catalog.buildTable(
                        TABLE,
                        new org.apache.iceberg.Schema(
                                Types.NestedField.required(1, "id", Types.LongType.get()),
                                Types.NestedField.optional(2, "payload", Types.VariantType.get())))
                .withProperty("format-version", "3")
                .create();
        Table table = catalog.loadTable(TABLE);

        SeaTunnelRuntimeException exception =
                assertThrows(SeaTunnelRuntimeException.class, () -> toCatalogTable(table));
        assertTrue(exception.getMessage().contains("variant"), exception.getMessage());

        CatalogTable projected = toCatalogTable(table, "select id from table");
        assertEquals(1, projected.getTableSchema().getColumns().size());
        assertEquals("id", projected.getTableSchema().getColumns().get(0).getName());
    }

    /**
     * Parquet files in a Hadoop catalog are read through Iceberg's Hadoop stream: Parquet 1.16
     * would open them with FileSystem.openFile(), which the Hadoop 3.1 runtime does not have.
     */
    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason = "Hadoop LocalFileSystem requires winutils on Windows")
    void readsParquetFilesOfHadoopCatalog(@TempDir Path warehouse) throws Exception {
        HadoopCatalog hadoopCatalog = new HadoopCatalog();
        hadoopCatalog.setConf(new Configuration());
        hadoopCatalog.initialize(
                "hadoop", Collections.singletonMap("warehouse", warehouse.toUri().toString()));
        try {
            Map<String, String> props = new HashMap<>(formatVersion("2"));
            props.put("write.format.default", "parquet");
            IcebergSinkConfig config = sinkConfig(props);
            Table table = SchemaUtils.autoCreateTable(hadoopCatalog, TABLE, config, idAndName());
            assertTrue(table.io() instanceof HadoopFileIO);
            Map<String, Object> options = new HashMap<>();
            options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
            options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
            IcebergSinkConfig upsertConfig = sinkConfig(props, options);
            SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
            commit(
                    table,
                    write(
                            table,
                            upsertConfig,
                            rowType,
                            new SeaTunnelRow(new Object[] {1L, "a"}),
                            new SeaTunnelRow(new Object[] {2L, "b"})));
            SeaTunnelRow update = new SeaTunnelRow(new Object[] {1L, "c"});
            update.setRowKind(RowKind.UPDATE_AFTER);
            // equality delete file, read back through the delete filter
            commit(table, write(table, upsertConfig, rowType, update));

            Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
            assertEquals(2, rows.size());
            assertEquals("c", rows.get(1L).getField(1));
            assertEquals("b", rows.get(2L).getField(1));

            DataFile dataFile =
                    table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
            try (SeekableInputStream stream =
                    new HadoopStreamFileIO(table.io())
                            .newInputFile(dataFile.location())
                            .newStream()) {
                // Parquet only unwraps Hadoop streams from a DelegatingInputStream; the stream is
                // only hidden on Hadoop runtimes without FileSystem.openFile()
                assertEquals(hadoopOpenFileAvailable(), stream instanceof DelegatingInputStream);
            }

            // the same through Iceberg's generic reader, with the FileIO set as io-impl
            Map<String, String> catalogProps = new HashMap<>();
            catalogProps.put("warehouse", warehouse.toUri().toString());
            catalogProps.put("io-impl", HadoopStreamFileIO.class.getName());
            HadoopCatalog streamCatalog = new HadoopCatalog();
            streamCatalog.setConf(new Configuration());
            streamCatalog.initialize("hadoop", catalogProps);
            try (CloseableIterable<Record> records =
                    IcebergGenerics.read(streamCatalog.loadTable(TABLE)).build()) {
                assertEquals(2, Iterables.size(records));
            } finally {
                streamCatalog.close();
            }
        } finally {
            hadoopCatalog.close();
        }
    }

    @Test
    void createsFormatVersion3TableWithPrimaryKeys() {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(formatVersion("3"), options);

        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        assertEquals(Collections.singleton("id"), table.schema().identifierFieldNames());
    }

    @Test
    void createsFormatVersion3TableWithSourcePrimaryKey() {
        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), Collections.emptyMap());
        config.put(IcebergSinkOptions.AUTO_CREATE_PROPS.key(), formatVersion("3"));
        TableSchema withPrimaryKey =
                TableSchema.builder()
                        .columns(idAndName().getColumns())
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "test", "db", "events"),
                        withPrimaryKey,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        Table table =
                SchemaUtils.autoCreateTable(
                        catalog,
                        TablePath.of("db", "events"),
                        catalogTable,
                        ReadonlyConfig.fromMap(config));
        assertEquals(Collections.singleton("id"), table.schema().identifierFieldNames());
    }

    @Test
    void supportsUpsertIntoExistingFormatVersion3Table() throws Exception {
        Table table =
                SchemaUtils.autoCreateTable(
                        catalog, TABLE, sinkConfig(formatVersion("3")), idAndName());
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(Collections.emptyMap(), options);

        commit(
                table,
                write(
                        table,
                        config,
                        idAndName().toPhysicalRowDataType(),
                        change(1, "a", RowKind.INSERT),
                        change(1, "b", RowKind.UPDATE_AFTER)));
        assertEquals("b", readAll(table, toCatalogTable(table)).get(1L).getField(1));
    }

    @Test
    void supportsWritesIntoFormatVersion3TableWithIdentifierFields() throws Exception {
        Table table =
                catalog.buildTable(
                                TABLE,
                                new org.apache.iceberg.Schema(
                                        Collections.singletonList(
                                                Types.NestedField.required(
                                                        1, "id", Types.LongType.get())),
                                        Collections.singleton(1)))
                        .withProperty("format-version", "3")
                        .create();
        IcebergSinkConfig config = sinkConfig(Collections.emptyMap());

        try (org.apache.iceberg.io.TaskWriter<Record> writer =
                new IcebergWriterFactory(loader(table), config).createTaskWriter(table, config)) {
            assertEquals(0, writer.complete().dataFiles().length);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void keepsUpsertSemantics(String version) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(formatVersion(version), options);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();

        SeaTunnelRow insert = new SeaTunnelRow(new Object[] {1L, "a"});
        SeaTunnelRow update = new SeaTunnelRow(new Object[] {1L, "b"});
        update.setRowKind(RowKind.UPDATE_AFTER);
        commit(table, write(table, config, rowType, insert, update));

        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(1, rows.size());
        assertEquals("b", rows.get(1L).getField(1));
    }

    @ParameterizedTest
    @CsvSource({
        "2,parquet,false", "3,parquet,false", "2,orc,false", "3,orc,false",
        "2,avro,false", "3,avro,false", "2,parquet,true", "3,parquet,true",
        "3,orc,true", "3,avro,true"
    })
    void writesPositionDeletesInTheTableFormat(String version, String format, boolean partitioned)
            throws Exception {
        IcebergSinkConfig config = deltaConfig(version, format, true, partitioned);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        List<WriteResult> results =
                write(
                        table,
                        config,
                        idAndName().toPhysicalRowDataType(),
                        change(1, "a", RowKind.INSERT),
                        change(1, "a", RowKind.UPDATE_AFTER),
                        change(2, "b", RowKind.INSERT),
                        change(2, "b", RowKind.DELETE),
                        change(3, "c", RowKind.INSERT));
        List<DeleteFile> positions = new ArrayList<>();
        results.forEach(
                result ->
                        result.getDeleteFiles().stream()
                                .filter(file -> file.content() == FileContent.POSITION_DELETES)
                                .forEach(positions::add));
        assertFalse(positions.isEmpty());
        assertEquals(2L, positions.stream().mapToLong(DeleteFile::recordCount).sum());
        if (version.equals("3")) {
            // Each data file has one vector; multiple partitions share one physical Puffin file.
            for (DeleteFile file : positions) {
                assertEquals(FileFormat.PUFFIN, file.format());
                assertTrue(file.location().endsWith(".puffin"));
                assertNotNull(file.referencedDataFile());
                assertTrue(file.contentOffset() > 0);
                assertTrue(file.contentSizeInBytes() > 0);
                assertTrue(
                        results.stream()
                                .flatMap(result -> result.getDataFiles().stream())
                                .anyMatch(
                                        data -> data.location().equals(file.referencedDataFile())));
            }
            assertEquals(1L, positions.stream().map(DeleteFile::location).distinct().count());
            assertEquals(
                    positions.size(),
                    positions.stream().map(DeleteFile::referencedDataFile).distinct().count());
        } else {
            positions.forEach(file -> assertEquals(FileFormat.fromString(format), file.format()));
        }
        commit(table, results);
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(1L).getField(1));
        assertFalse(rows.containsKey(2L));
        assertEquals("c", rows.get(3L).getField(1));
    }

    @ParameterizedTest
    @CsvSource({
        "2,false,false",
        "3,false,false",
        "2,true,false",
        "3,true,false",
        "2,false,true",
        "3,false,true",
        "2,true,true",
        "3,true,true"
    })
    void preservesCdcAndRecoveryAcrossCheckpoints(
            String version, boolean upsert, boolean partitioned) throws Exception {
        IcebergSinkConfig config = deltaConfig(version, "parquet", upsert, partitioned);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        List<WriteResult> first =
                write(
                        table,
                        config,
                        rowType,
                        change(1, "a", RowKind.INSERT),
                        change(2, "b", RowKind.INSERT),
                        change(2, "b", RowKind.DELETE),
                        change(2, "b", RowKind.INSERT));
        // Restore an uncommitted serialized checkpoint, then replay after its external commit.
        IcebergAggregatedCommitInfo checkpoint = checkpoint(first, 101);
        restore(table, config, checkpoint);
        long firstSnapshot = table.currentSnapshot().snapshotId();
        restore(table, config, checkpoint);
        assertEquals(firstSnapshot, table.currentSnapshot().snapshotId());
        assertEquals(2, readAll(table, toCatalogTable(table)).size());

        List<WriteResult> second =
                write(
                        table,
                        config,
                        rowType,
                        change(1, "a", RowKind.UPDATE_BEFORE),
                        change(1, "updated", RowKind.UPDATE_AFTER),
                        change(2, "b", RowKind.DELETE),
                        change(3, "c", RowKind.INSERT),
                        change(3, "c", RowKind.UPDATE_BEFORE),
                        change(3, "c", RowKind.UPDATE_AFTER));
        restore(table, config, checkpoint(second, 102));
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(2, rows.size());
        assertEquals("updated", rows.get(1L).getField(1));
        assertEquals("c", rows.get(3L).getField(1));
        assertEquals(2, Iterables.size(table.snapshots()));
    }

    @ParameterizedTest
    @CsvSource({"2,false", "2,true", "3,false", "3,true"})
    void ordinaryCommitReplayAndReusedCheckpointIds(String version, boolean cdc) throws Exception {
        IcebergSinkConfig config = deltaConfig(version, "parquet", cdc, false);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        List<WriteResult> first =
                cdc
                        ? write(
                                table,
                                config,
                                rowType,
                                change(1, "old", RowKind.INSERT),
                                change(1, "new", RowKind.UPDATE_AFTER))
                        : write(table, config, rowType, change(1, "new", RowKind.INSERT));
        IcebergAggregatedCommitInfo saved = checkpoint(first, 1);
        publish(table, config, saved, false);
        long firstSnapshot = table.currentSnapshot().snapshotId();
        publish(table, config, checkpoint(first, 1), false);
        publish(table, config, checkpoint(first, 1), true);
        assertEquals(firstSnapshot, table.currentSnapshot().snapshotId());
        assertEquals(1, readAll(table, toCatalogTable(table)).size());

        // A separate job starts numbering checkpoints at 1 again, but creates new immutable files.
        List<WriteResult> second =
                write(table, config, rowType, change(2, "second-job", RowKind.INSERT));
        publish(table, config, checkpoint(second, 1), false);
        publish(table, config, saved, false);
        assertEquals(2, Iterables.size(table.snapshots()));
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(2, rows.size());
        assertEquals("new", rows.get(1L).getField(1));
        assertEquals("second-job", rows.get(2L).getField(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "avro"})
    void readsLegacyPositionDeletesAfterUpgradeToV3(String format) throws Exception {
        IcebergSinkConfig v2 = deltaConfig("2", format, true, false);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, v2, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        List<WriteResult> legacy =
                write(
                        table,
                        v2,
                        rowType,
                        change(1, "old", RowKind.INSERT),
                        change(1, "before-upgrade", RowKind.UPDATE_AFTER),
                        change(2, "removed", RowKind.INSERT),
                        change(4, "old-untouched", RowKind.INSERT),
                        change(4, "untouched", RowKind.UPDATE_AFTER));
        commit(table, legacy);
        assertTrue(
                legacy.stream()
                        .flatMap(result -> result.getDeleteFiles().stream())
                        .anyMatch(file -> file.content() == FileContent.POSITION_DELETES));
        table.updateProperties().set("format-version", "3").commit();
        IcebergSinkConfig v3 = deltaConfig("3", format, true, false);
        restore(
                table,
                v3,
                checkpoint(
                        write(
                                table,
                                v3,
                                rowType,
                                change(1, "after-upgrade", RowKind.UPDATE_AFTER),
                                change(2, "removed", RowKind.DELETE),
                                change(3, "old", RowKind.INSERT),
                                change(3, "new", RowKind.UPDATE_AFTER)),
                        103));
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(3, rows.size());
        assertEquals("after-upgrade", rows.get(1L).getField(1));
        assertEquals("new", rows.get(3L).getField(1));
        assertEquals("untouched", rows.get(4L).getField(1));
    }

    @ParameterizedTest
    @CsvSource({"2,parquet", "3,parquet", "2,orc", "3,orc", "2,avro", "3,avro"})
    void cdcMovesPartitionsAndChangesKeys(String version, String format) throws Exception {
        Map<String, String> props = new HashMap<>(formatVersion(version));
        props.put("write.format.default", format);
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS.key(), "name");
        IcebergSinkConfig config = sinkConfig(props, options);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        commit(
                table,
                write(
                        table,
                        config,
                        rowType,
                        change(1, "old", RowKind.INSERT),
                        change(1, "old", RowKind.UPDATE_BEFORE),
                        change(1, "new", RowKind.UPDATE_AFTER)));
        assertEquals("new", readAll(table, toCatalogTable(table)).get(1L).getField(1));
        restore(
                table,
                config,
                checkpoint(
                        write(
                                table,
                                config,
                                rowType,
                                change(1, "new", RowKind.UPDATE_BEFORE),
                                change(2, "final", RowKind.UPDATE_AFTER)),
                        104));
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(1, rows.size());
        assertEquals("final", rows.get(2L).getField(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void restoresCombinedWriterResultsWithoutDuplicatingVectors(String version) throws Exception {
        IcebergSinkConfig config = deltaConfig(version, "parquet", true, true);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        List<WriteResult> results = new ArrayList<>();
        for (int writer = 0; writer < 3; writer++) {
            results.addAll(
                    write(
                            table,
                            config,
                            rowType,
                            change(writer, "before", RowKind.INSERT),
                            change(writer, "after", RowKind.UPDATE_AFTER)));
        }
        IcebergAggregatedCommitInfo saved = checkpoint(results, 105);
        restore(table, config, saved);
        long snapshotId = table.currentSnapshot().snapshotId();
        restore(table, config, saved);
        assertEquals(snapshotId, table.currentSnapshot().snapshotId());
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(3, rows.size());
        rows.values().forEach(row -> assertEquals("after", row.getField(1)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void rollsFilesWithoutLosingDeletePositions(String version) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        options.put(
                IcebergSinkOptions.WRITE_PROPS.key(),
                Collections.singletonMap("write.target-file-size-bytes", "1"));
        IcebergSinkConfig config = sinkConfig(formatVersion(version), options);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        RecordWriter writer = new IcebergWriterFactory(loader(table), config).createWriter(null);
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        for (int id = 0; id < 2100; id++) {
            writer.write(change(id, "initial", RowKind.INSERT), rowType);
        }
        writer.write(change(0, "updated", RowKind.UPDATE_AFTER), rowType);
        writer.write(change(999, "initial", RowKind.DELETE), rowType);
        writer.write(change(1000, "updated", RowKind.UPDATE_AFTER), rowType);
        writer.write(change(2099, "initial", RowKind.DELETE), rowType);
        List<WriteResult> results = writer.complete();
        assertTrue(results.get(0).getDataFiles().size() >= 3);
        commit(table, results);
        Map<Long, SeaTunnelRow> rows = readAll(table, toCatalogTable(table));
        assertEquals(2098, rows.size());
        assertEquals("updated", rows.get(0L).getField(1));
        assertEquals("updated", rows.get(1000L).getField(1));
        assertFalse(rows.containsKey(999L));
        assertFalse(rows.containsKey(2099L));
    }

    @Test
    void abortRemovesOnlyNewFilesIncludingSharedPuffin() throws Exception {
        IcebergSinkConfig config = deltaConfig("3", "parquet", true, true);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        List<WriteResult> committed =
                write(
                        table,
                        config,
                        idAndName().toPhysicalRowDataType(),
                        change(10, "committed", RowKind.INSERT));
        commit(table, committed);
        TaskWriter<Record> writer =
                new IcebergWriterFactory(loader(table), config).createTaskWriter(table, config);
        for (long id = 0; id < 8; id++) {
            writer.write(taskRow(table, id, "before", RowKind.INSERT));
            writer.write(taskRow(table, id, "after", RowKind.UPDATE_AFTER));
        }
        org.apache.iceberg.io.WriteResult result = writer.complete();
        assertTrue(
                Arrays.stream(result.deleteFiles())
                                .filter(file -> file.format() == FileFormat.PUFFIN)
                                .count()
                        > 1);
        writer.abort();
        for (DataFile file : result.dataFiles()) {
            assertThrows(NotFoundException.class, () -> table.io().newInputFile(file.location()));
        }
        for (DeleteFile file : result.deleteFiles()) {
            assertThrows(NotFoundException.class, () -> table.io().newInputFile(file.location()));
        }
        for (DataFile file : committed.get(0).getDataFiles()) {
            assertTrue(table.io().newInputFile(file.location()).exists());
        }
        assertEquals(1, readAll(table, toCatalogTable(table)).size());
        assertThrows(
                IOException.class,
                () -> writer.write(taskRow(table, 11, "closed", RowKind.INSERT)));
    }

    @Test
    void failedVectorCloseCannotCommitAndStillCleansUp() throws Exception {
        IcebergSinkConfig config = deltaConfig("3", "parquet", true, false);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        FileIO io = Mockito.spy(table.io());
        List<String> paths = new ArrayList<>();
        when(io.newOutputFile(anyString()))
                .thenAnswer(
                        invocation -> {
                            String path = invocation.getArgument(0);
                            paths.add(path);
                            return table.io().newOutputFile(path);
                        });
        Table withTrackedIO = Mockito.spy(table);
        doReturn(io).when(withTrackedIO).io();
        try (MockedConstruction<BaseDVFileWriter> ignored =
                Mockito.mockConstruction(
                        BaseDVFileWriter.class,
                        (writer, context) ->
                                doThrow(new IOException("injected vector close failure"))
                                        .when(writer)
                                        .close())) {
            TaskWriter<Record> writer =
                    new IcebergWriterFactory(loader(withTrackedIO), config)
                            .createTaskWriter(withTrackedIO, config);
            writer.write(taskRow(table, 1, "before", RowKind.INSERT));
            writer.write(taskRow(table, 1, "after", RowKind.UPDATE_AFTER));
            assertThrows(IOException.class, writer::complete);
            writer.abort();
            assertFalse(paths.isEmpty());
            for (String path : paths) {
                assertThrows(NotFoundException.class, () -> io.newInputFile(path), path);
            }
            assertNull(table.currentSnapshot());
            assertThrows(IOException.class, writer::complete);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partialPuffinWritePreventsCommitAndCanBeCleanedUp(boolean closeFailure) throws Exception {
        IcebergSinkConfig config = deltaConfig("3", "parquet", true, false);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        FileIO io = Mockito.spy(table.io());
        List<String> paths = new ArrayList<>();
        when(io.newOutputFile(anyString()))
                .thenAnswer(
                        invocation -> {
                            String path = invocation.getArgument(0);
                            paths.add(path);
                            OutputFile output = table.io().newOutputFile(path);
                            if (!path.endsWith(".puffin")) {
                                return output;
                            }
                            OutputFile failing = Mockito.spy(output);
                            Mockito.doAnswer(
                                            ignored -> {
                                                PositionOutputStream delegate = output.create();
                                                return new PositionOutputStream() {
                                                    @Override
                                                    public long getPos() throws IOException {
                                                        return delegate.getPos();
                                                    }

                                                    @Override
                                                    public void write(int value)
                                                            throws IOException {
                                                        delegate.write(value);
                                                        throw new IOException(
                                                                "injected partial Puffin write");
                                                    }

                                                    @Override
                                                    public void close() throws IOException {
                                                        delegate.close();
                                                        if (closeFailure) {
                                                            throw new IOException(
                                                                    "injected stream close failure");
                                                        }
                                                    }
                                                };
                                            })
                                    .when(failing)
                                    .create();
                            return failing;
                        });
        Table withTrackedIO = Mockito.spy(table);
        doReturn(io).when(withTrackedIO).io();
        TaskWriter<Record> writer =
                new IcebergWriterFactory(loader(withTrackedIO), config)
                        .createTaskWriter(withTrackedIO, config);
        writer.write(taskRow(table, 1, "before", RowKind.INSERT));
        writer.write(taskRow(table, 1, "after", RowKind.UPDATE_AFTER));
        IOException failure = assertThrows(IOException.class, writer::complete);
        if (closeFailure) {
            assertTrue(
                    Arrays.stream(failure.getSuppressed())
                            .anyMatch(
                                    error ->
                                            "injected stream close failure"
                                                    .equals(error.getMessage())));
        }
        String puffin = paths.stream().filter(path -> path.endsWith(".puffin")).findFirst().get();
        assertTrue(
                io.newInputFile(puffin).getLength() > 0, "failure must leave a real partial file");
        assertNull(table.currentSnapshot());
        writer.abort();
        for (String path : paths) {
            assertThrows(NotFoundException.class, () -> io.newInputFile(path), path);
        }
        assertThrows(IOException.class, writer::complete);
    }

    @ParameterizedTest
    @CsvSource({"parquet,ts_nanos", "orc,day(ts_nanos)", "avro,ts_nanos"})
    void cdcUsesNanosecondPartitionAndEqualityKeys(String format, String partition)
            throws Exception {
        Map<String, String> props = new HashMap<>(formatVersion("3"));
        props.put("write.format.default", format);
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id,ts_nanos");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        options.put(IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS.key(), partition);
        IcebergSinkConfig config = sinkConfig(props, options);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, timestampSchema());
        commit(
                table,
                write(
                        table,
                        config,
                        timestampRowType(),
                        row(1, TS_1, TSTZ_1, RowKind.INSERT),
                        row(1, TS_1, TSTZ_2, RowKind.UPDATE_AFTER)));
        assertEquals(
                TSTZ_2.toInstant(),
                ((OffsetDateTime) readAll(table, toCatalogTable(table)).get(1L).getField(2))
                        .toInstant());
        commit(
                table,
                write(table, config, timestampRowType(), row(1, TS_1, TSTZ_2, RowKind.DELETE)));
        assertTrue(readAll(table, toCatalogTable(table)).isEmpty());
        // Equality keys must still be loaded when the source projects them out.
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                IcebergFileScanTaskReader reader =
                        IcebergFileScanTaskReader.builder()
                                .fileIO(table.io())
                                .tableSchema(table.schema())
                                .projectedSchema(table.schema().select("id"))
                                .caseSensitive(true)
                                .reuseContainers(true)
                                .build();
                try (CloseableIterator<Record> records = reader.open(task)) {
                    assertFalse(records.hasNext());
                }
            }
        }
    }

    @Test
    void nanosecondEqualityDeletesCopyReusedBinaryKeys() throws Exception {
        Map<String, String> props = new HashMap<>(formatVersion("3"));
        props.put("write.format.default", "avro");
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "ts_nanos,binary_key");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(props, options);
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                        .column(timestamp("ts_nanos", LocalTimeType.LOCAL_DATE_TIME_TYPE, 9))
                        .column(
                                PhysicalColumn.of(
                                        "binary_key",
                                        PrimitiveByteArrayType.INSTANCE,
                                        (Long) null,
                                        false,
                                        null,
                                        null))
                        .build();
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, schema);
        SeaTunnelRow first = new SeaTunnelRow(new Object[] {1L, TS_1, new byte[] {1}});
        SeaTunnelRow second = new SeaTunnelRow(new Object[] {2L, TS_1, new byte[] {2}});
        commit(table, write(table, config, schema.toPhysicalRowDataType(), first, second));
        assertEquals(2, readAll(table, toCatalogTable(table)).size());

        first.setRowKind(RowKind.DELETE);
        second.setRowKind(RowKind.DELETE);
        List<WriteResult> results =
                write(table, config, schema.toPhysicalRowDataType(), first, second);
        DeleteFile deletes =
                Iterables.getOnlyElement(Iterables.getOnlyElement(results).getDeleteFiles());
        assertEquals(FileContent.EQUALITY_DELETES, deletes.content());
        assertEquals(FileFormat.AVRO, deletes.format());
        assertEquals(2, deletes.recordCount());
        commit(table, results);

        List<Long> remaining = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                IcebergFileScanTaskReader reader =
                        IcebergFileScanTaskReader.builder()
                                .fileIO(table.io())
                                .tableSchema(table.schema())
                                .projectedSchema(table.schema().select("id"))
                                .caseSensitive(true)
                                .reuseContainers(true)
                                .build();
                try (CloseableIterator<Record> records = reader.open(task)) {
                    while (records.hasNext()) {
                        remaining.add((Long) records.next().getField("id"));
                    }
                }
            }
        }
        assertEquals(Collections.emptyList(), remaining, "Both binary keys must be deleted");
    }

    private static IcebergRecord taskRow(Table table, long id, String name, RowKind kind) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", id);
        record.setField("name", name);
        return new IcebergRecord(record, kind);
    }

    private static IcebergSinkConfig deltaConfig(
            String version, String format, boolean upsert, boolean partitioned) {
        Map<String, String> props = new HashMap<>(formatVersion(version));
        props.put("write.format.default", format);
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), upsert);
        if (partitioned) {
            // Stable partition transform of the primary key, also valid for upsert mode.
            options.put(IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS.key(), "bucket(id, 4)");
        }
        return sinkConfig(props, options);
    }

    private static IcebergAggregatedCommitInfo checkpoint(List<WriteResult> results, long id)
            throws Exception {
        IcebergAggregatedCommitInfo info =
                new IcebergAggregatedCommitInfo(
                        Collections.singletonList(new IcebergCommitInfo(results, id)));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(info);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (IcebergAggregatedCommitInfo) in.readObject();
        }
    }

    private static void restore(
            Table table, IcebergSinkConfig config, IcebergAggregatedCommitInfo info)
            throws Exception {
        publish(table, config, info, true);
    }

    private static void publish(
            Table table,
            IcebergSinkConfig config,
            IcebergAggregatedCommitInfo info,
            boolean restore)
            throws Exception {
        try (MockedConstruction<IcebergTableLoader> ignored =
                Mockito.mockConstruction(
                        IcebergTableLoader.class,
                        (loader, context) -> {
                            when(loader.loadTable()).thenReturn(table);
                            when(loader.getTableIdentifier()).thenReturn(TABLE);
                        })) {
            IcebergAggregatedCommitter committer = new IcebergAggregatedCommitter(config, null);
            committer.init();
            try {
                if (restore) {
                    committer.restoreCommit(Collections.singletonList(info));
                } else {
                    committer.commit(Collections.singletonList(info));
                }
            } finally {
                committer.close();
            }
        }
        table.refresh();
    }

    @Test
    void keepsNanosecondColumnOnTimestampModifyEvent() {
        Table table =
                SchemaUtils.autoCreateTable(
                        catalog, TABLE, sinkConfig(formatVersion("3")), timestampSchema());
        SchemaChangeWrapper wrapper = new SchemaChangeWrapper();
        wrapper.modifyColumn("ts_nanos", Types.TimestampType.withoutZone());

        SchemaUtils.applySchemaUpdates(table, wrapper);

        table.refresh();
        assertEquals(Types.TimestampNanoType.withoutZone(), table.schema().findType("ts_nanos"));
    }

    private static List<WriteResult> write(
            Table table, IcebergSinkConfig config, SeaTunnelRowType rowType, SeaTunnelRow... rows) {
        RecordWriter writer = new IcebergWriterFactory(loader(table), config).createWriter(null);
        for (SeaTunnelRow row : rows) {
            writer.write(row, rowType);
        }
        return writer.complete();
    }

    private static void commit(Table table, List<WriteResult> results) {
        RowDelta rowDelta = table.newRowDelta();
        results.forEach(
                result -> {
                    result.getDataFiles().forEach(rowDelta::addRows);
                    result.getDeleteFiles().forEach(rowDelta::addDeletes);
                });
        rowDelta.commit();
    }

    private static Map<Long, SeaTunnelRow> readAll(Table table, CatalogTable catalogTable)
            throws IOException {
        table.refresh();
        SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        DefaultDeserializer deserializer = new DefaultDeserializer(rowType, table.schema());
        Map<Long, SeaTunnelRow> rows = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                IcebergFileScanTaskReader reader =
                        IcebergFileScanTaskReader.builder()
                                .fileIO(table.io())
                                .tableSchema(table.schema())
                                .projectedSchema(table.schema())
                                .caseSensitive(true)
                                .build();
                try (CloseableIterator<Record> records = reader.open(task)) {
                    while (records.hasNext()) {
                        SeaTunnelRow row = deserializer.deserialize(records.next());
                        assertNull(
                                rows.put((Long) row.getField(0), row),
                                "duplicate live primary key");
                    }
                }
            }
        }
        return rows;
    }

    private CatalogTable toCatalogTable(Table table) throws Exception {
        return toCatalogTable(table, null);
    }

    private CatalogTable toCatalogTable(Table table, String query) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), Collections.emptyMap());
        if (query != null) {
            config.put(IcebergCommonOptions.KEY_TABLE.key(), "events");
            config.put(IcebergSourceOptions.QUERY.key(), query);
        }
        try (MockedConstruction<IcebergCatalogLoader> ignored =
                Mockito.mockConstruction(
                        IcebergCatalogLoader.class,
                        (loader, context) -> when(loader.loadCatalog()).thenReturn(catalog))) {
            IcebergCatalog icebergCatalog =
                    new IcebergCatalog("test", ReadonlyConfig.fromMap(config));
            return icebergCatalog.toCatalogTable(table, TablePath.of("db", "events"));
        }
    }

    private static Column column(CatalogTable catalogTable, String name) {
        return catalogTable.getTableSchema().getColumns().stream()
                .filter(column -> column.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing column " + name));
    }

    private static IcebergTableLoader loader(Table table) {
        IcebergTableLoader loader = mock(IcebergTableLoader.class);
        when(loader.loadTable()).thenReturn(table);
        return loader;
    }

    private static SeaTunnelRow row(
            long id, LocalDateTime ts, OffsetDateTime tstz, RowKind rowKind) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {id, ts, tstz, ts.withNano(123456000), ts.withNano(0)});
        row.setRowKind(rowKind);
        return row;
    }

    private static SeaTunnelRow change(long id, String name, RowKind kind) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(kind);
        return row;
    }

    private static TableSchema idAndName() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                .column(
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, (Long) null, true, null, null))
                .build();
    }

    private static TableSchema timestampSchema() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                .column(timestamp("ts_nanos", LocalTimeType.LOCAL_DATE_TIME_TYPE, 9))
                .column(timestamp("tstz_nanos", LocalTimeType.OFFSET_DATE_TIME_TYPE, 9))
                .column(timestamp("ts_micros", LocalTimeType.LOCAL_DATE_TIME_TYPE, 6))
                .column(timestamp("ts_default", LocalTimeType.LOCAL_DATE_TIME_TYPE, null))
                .build();
    }

    private static SeaTunnelRowType timestampRowType() {
        return timestampSchema().toPhysicalRowDataType();
    }

    private static PhysicalColumn timestamp(String name, LocalTimeType<?> type, Integer scale) {
        return PhysicalColumn.of(name, type, (Long) null, scale, true, null, null);
    }

    private static Map<String, String> formatVersion(String formatVersion) {
        return Collections.singletonMap("format-version", formatVersion);
    }

    private static IcebergSinkConfig sinkConfig(Map<String, String> autoCreateProps) {
        return sinkConfig(autoCreateProps, Collections.emptyMap());
    }

    private static IcebergSinkConfig sinkConfig(
            Map<String, String> autoCreateProps, String partitionKeys) {
        return sinkConfig(
                autoCreateProps,
                Collections.singletonMap(
                        IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS.key(), partitionKeys));
    }

    private static IcebergSinkConfig sinkConfig(
            Map<String, String> autoCreateProps, Map<String, Object> options) {
        Map<String, Object> config = new HashMap<>(options);
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), Collections.emptyMap());
        config.put(IcebergSinkOptions.AUTO_CREATE_PROPS.key(), autoCreateProps);
        config.put(IcebergCommonOptions.KEY_TABLE.key(), "events");
        config.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "db");
        return new IcebergSinkConfig(ReadonlyConfig.fromMap(config));
    }

    private static boolean hadoopOpenFileAvailable() {
        try {
            org.apache.hadoop.fs.FileSystem.class.getMethod(
                    "openFile", org.apache.hadoop.fs.Path.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static int formatVersion(Table table) {
        return ((HasTableOperations) table).operations().current().formatVersion();
    }
}
