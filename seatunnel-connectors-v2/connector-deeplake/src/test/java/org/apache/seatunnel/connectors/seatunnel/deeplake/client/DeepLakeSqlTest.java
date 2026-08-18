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

package org.apache.seatunnel.connectors.seatunnel.deeplake.client;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeepLakeSqlTest {

    @Test
    void buildsCreateAndInsertStatementsForSupportedTypes() {
        CatalogTable table = catalogTable();

        assertEquals(
                "CREATE TABLE IF NOT EXISTS \"research\".\"documents\" "
                        + "(\"document_id\" BIGINT NOT NULL, \"content\" TEXT, \"embedding\" FLOAT4[], "
                        + "\"payload\" BYTEA, \"score\" NUMERIC(12, 4), PRIMARY KEY (\"document_id\")) "
                        + "USING deeplake",
                DeepLakeSql.createTableSql("research", "documents", table));
        assertEquals(
                "INSERT INTO \"research\".\"documents\" "
                        + "(\"document_id\", \"content\", \"embedding\", \"payload\", \"score\") "
                        + "VALUES ($1, $2, $3::float4[], decode($4, 'base64'), $5)",
                DeepLakeSql.insertSql("research", "documents", table.getSeaTunnelRowType()));
    }

    @Test
    void quotesWorkspaceTableAndColumnIdentifiers() {
        assertEquals("\"team\"\"one\"", DeepLakeSql.quoteIdentifier("team\"one"));
        assertEquals(
                "\"team\"\"one\".\"docs\"\"2026\"",
                DeepLakeSql.qualifiedTable("team\"one", "docs\"2026"));
    }

    @Test
    void rejectsVectorEncodingsThatCannotBeRepresentedWithoutConversion() {
        assertThrows(
                DeepLakeConnectorException.class,
                () -> DeepLakeSql.toDeepLakeType(VectorType.VECTOR_FLOAT16_TYPE));
    }

    @Test
    void rejectsBinaryValuesInsideArrays() {
        assertThrows(
                DeepLakeConnectorException.class,
                () ->
                        DeepLakeSql.toDeepLakeType(
                                new ArrayType<>(byte[][].class, PrimitiveByteArrayType.INSTANCE)));
    }

    static CatalogTable catalogTable() {
        List<Column> columns =
                Arrays.asList(
                        column("document_id", BasicType.LONG_TYPE, false),
                        column("content", BasicType.STRING_TYPE, true),
                        column("embedding", VectorType.VECTOR_FLOAT_TYPE, true),
                        column("payload", PrimitiveByteArrayType.INSTANCE, true),
                        column("score", new DecimalType(12, 4), true));
        TableSchema schema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(
                                PrimaryKey.of(
                                        "pk_documents", Collections.singletonList("document_id")))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("deeplake", "research", "documents"),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "Deep Lake documents");
    }

    private static Column column(
            String name,
            org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> dataType,
            boolean nullable) {
        return PhysicalColumn.builder().name(name).dataType(dataType).nullable(nullable).build();
    }
}
