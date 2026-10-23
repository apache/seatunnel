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

package org.apache.seatunnel.connectors.cdc.base.utils;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;

import java.util.Collections;

/** Unit tests for binlog/GTID extractor methods in {@link SourceRecordUtils}. */
public class SourceRecordUtilsTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Build a minimal Debezium-style value schema with a source struct. */
    private Schema buildValueSchema(Schema sourceSchema) {
        return SchemaBuilder.struct()
                .name("test.Envelope")
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    /** Build a source schema with typical MySQL-CDC fields. */
    private Schema buildMysqlSourceSchema(boolean withGtid) {
        SchemaBuilder builder =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.Source")
                        .field("file", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("pos", Schema.OPTIONAL_INT64_SCHEMA)
                        .field("row", Schema.OPTIONAL_INT32_SCHEMA)
                        .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
        if (withGtid) {
            builder.field("gtid", Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder.build();
    }

    /** Build a source schema without MySQL-specific fields (e.g. PostgreSQL). */
    private Schema buildNonMysqlSourceSchema() {
        return SchemaBuilder.struct()
                .name("io.debezium.connector.postgresql.Source")
                .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    private SourceRecord buildRecord(Schema valueSchema, Struct value) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test-topic",
                null,
                null,
                null,
                valueSchema,
                value);
    }

    // -----------------------------------------------------------------------
    // getBinlogFile
    // -----------------------------------------------------------------------

    @Test
    public void testGetBinlogFile_returnsValue() {
        Schema sourceSchema = buildMysqlSourceSchema(false);
        Struct source =
                new Struct(sourceSchema)
                        .put("file", "mysql-bin-changelog.000123")
                        .put("pos", 4521L)
                        .put("row", 0);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertEquals(
                "mysql-bin-changelog.000123", SourceRecordUtils.getBinlogFile(record));
    }

    @Test
    public void testGetBinlogFile_nullForEmptyString() {
        // Snapshot rows: Debezium sets file = "" (empty string)
        Schema sourceSchema = buildMysqlSourceSchema(false);
        Struct source = new Struct(sourceSchema).put("file", "").put("pos", 0L).put("row", 0);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "r");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getBinlogFile(record));
    }

    @Test
    public void testGetBinlogFile_nullForNonMysqlConnector() {
        Schema sourceSchema = buildNonMysqlSourceSchema();
        Struct source = new Struct(sourceSchema).put("lsn", 12345L);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getBinlogFile(record));
    }

    // -----------------------------------------------------------------------
    // getBinlogPos
    // -----------------------------------------------------------------------

    @Test
    public void testGetBinlogPos_returnsValue() {
        Schema sourceSchema = buildMysqlSourceSchema(false);
        Struct source =
                new Struct(sourceSchema)
                        .put("file", "mysql-bin-changelog.000123")
                        .put("pos", 54321L)
                        .put("row", 0);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertEquals(Long.valueOf(54321L), SourceRecordUtils.getBinlogPos(record));
    }

    @Test
    public void testGetBinlogPos_nullForNonMysqlConnector() {
        Schema sourceSchema = buildNonMysqlSourceSchema();
        Struct source = new Struct(sourceSchema).put("lsn", 12345L);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getBinlogPos(record));
    }

    // -----------------------------------------------------------------------
    // getBinlogRow
    // -----------------------------------------------------------------------

    @Test
    public void testGetBinlogRow_returnsValue() {
        Schema sourceSchema = buildMysqlSourceSchema(false);
        Struct source =
                new Struct(sourceSchema)
                        .put("file", "mysql-bin-changelog.000123")
                        .put("pos", 4521L)
                        .put("row", 2);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertEquals(Integer.valueOf(2), SourceRecordUtils.getBinlogRow(record));
    }

    @Test
    public void testGetBinlogRow_nullForNonMysqlConnector() {
        Schema sourceSchema = buildNonMysqlSourceSchema();
        Struct source = new Struct(sourceSchema).put("lsn", 12345L);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getBinlogRow(record));
    }

    // -----------------------------------------------------------------------
    // getGtid
    // -----------------------------------------------------------------------

    @Test
    public void testGetGtid_returnsValue() {
        Schema sourceSchema = buildMysqlSourceSchema(true);
        Struct source =
                new Struct(sourceSchema)
                        .put("file", "mysql-bin-changelog.000123")
                        .put("pos", 4521L)
                        .put("row", 0)
                        .put("gtid", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23");

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertEquals(
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:23", SourceRecordUtils.getGtid(record));
    }

    @Test
    public void testGetGtid_nullWhenGtidDisabled() {
        // Schema has no gtid field — GTID disabled on server
        Schema sourceSchema = buildMysqlSourceSchema(false);
        Struct source =
                new Struct(sourceSchema)
                        .put("file", "mysql-bin-changelog.000123")
                        .put("pos", 4521L)
                        .put("row", 0);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getGtid(record));
    }

    @Test
    public void testGetGtid_nullForEmptyString() {
        // Snapshot rows: gtid field present but empty
        Schema sourceSchema = buildMysqlSourceSchema(true);
        Struct source =
                new Struct(sourceSchema)
                        .put("file", "")
                        .put("pos", 0L)
                        .put("row", 0)
                        .put("gtid", "");

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "r");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getGtid(record));
    }

    @Test
    public void testGetGtid_nullForNonMysqlConnector() {
        Schema sourceSchema = buildNonMysqlSourceSchema();
        Struct source = new Struct(sourceSchema).put("lsn", 12345L);

        Schema valueSchema = buildValueSchema(sourceSchema);
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, "c");

        SourceRecord record = buildRecord(valueSchema, value);
        Assertions.assertNull(SourceRecordUtils.getGtid(record));
    }

    /**
     * Vitess exposes the logical database as keyspace, so table identity must still resolve even
     * when Debezium leaves database_name empty or blank.
     */
    @Test
    public void testGetTablePathUsesKeyspaceWhenDatabaseNameIsMissing() {
        assertVitessTableIdentity(null);
        assertVitessTableIdentity("");
    }

    /**
     * Builds a minimal Vitess-like source record and verifies both SeaTunnel and Debezium table
     * identifiers fall back to keyspace when the generic database field is absent.
     */
    private static void assertVitessTableIdentity(String databaseName) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(
                                AbstractSourceInfo.DATABASE_NAME_KEY,
                                SchemaBuilder.string().optional().build())
                        .field("keyspace", SchemaBuilder.string().build())
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, SchemaBuilder.string().build())
                        .build();
        Struct sourceStruct =
                new Struct(sourceSchema)
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, databaseName)
                        .put("keyspace", "inventory")
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, "products");
        Schema valueSchema =
                SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, sourceSchema).build();
        Struct valueStruct = new Struct(valueSchema).put(Envelope.FieldName.SOURCE, sourceStruct);
        SourceRecord record =
                new SourceRecord(null, null, "vitess", null, null, valueSchema, valueStruct);

        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        TableId tableId = SourceRecordUtils.getTableId(record);

        Assertions.assertEquals(TablePath.of("inventory", null, "products"), tablePath);
        Assertions.assertEquals(new TableId("inventory", null, "products"), tableId);
    }
}
