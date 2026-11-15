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
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeWrapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergWriterFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.reader.IcebergFileScanTaskReader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.HadoopStreamFileIO;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    void rejectsCreatingFormatVersion3TableWithPrimaryKeys() {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(formatVersion("3"), options);

        assertUpsertUnsupported(
                () -> SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName()));
        assertFalse(catalog.tableExists(TABLE));
    }

    @Test
    void rejectsCreatingFormatVersion3TableWithSourcePrimaryKey() {
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

        assertUpsertUnsupported(
                () ->
                        SchemaUtils.autoCreateTable(
                                catalog,
                                TablePath.of("db", "events"),
                                catalogTable,
                                ReadonlyConfig.fromMap(config)));
        assertFalse(catalog.tableExists(TABLE));
    }

    @Test
    void rejectsUpsertIntoExistingFormatVersion3Table() {
        Table table =
                SchemaUtils.autoCreateTable(
                        catalog, TABLE, sinkConfig(formatVersion("3")), idAndName());
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(Collections.emptyMap(), options);

        assertUpsertUnsupported(
                () ->
                        new IcebergWriterFactory(loader(table), config)
                                .createTaskWriter(table, config));
    }

    private static void assertUpsertUnsupported(Executable executable) {
        IcebergConnectorException exception =
                assertThrows(IcebergConnectorException.class, executable);
        assertEquals(
                IcebergConnectorErrorCode.UNSUPPORTED_WRITE_FOR_TABLE_FORMAT_VERSION,
                exception.getSeaTunnelErrorCode());
        assertTrue(
                exception
                        .getMessage()
                        .contains("Upsert and CDC writes are not supported for Iceberg tables"),
                exception.getMessage());
    }

    @Test
    void rejectsWritesIntoFormatVersion3TableWithIdentifierFields() {
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

        assertThrows(
                IcebergConnectorException.class,
                () ->
                        new IcebergWriterFactory(loader(table), config)
                                .createTaskWriter(table, config));
    }

    @Test
    void keepsUpsertIntoFormatVersion2Table() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        IcebergSinkConfig config = sinkConfig(Collections.emptyMap(), options);
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
                        rows.put((Long) row.getField(0), row);
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
