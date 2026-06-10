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

/** Covers table path extraction for connectors that do not populate database_name. */
class SourceRecordUtilsTest {

    /**
     * Vitess exposes the logical database as keyspace, so table identity must still resolve even
     * when Debezium leaves database_name empty.
     */
    @Test
    void testGetTablePathUsesKeyspaceWhenDatabaseNameIsMissing() {
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
