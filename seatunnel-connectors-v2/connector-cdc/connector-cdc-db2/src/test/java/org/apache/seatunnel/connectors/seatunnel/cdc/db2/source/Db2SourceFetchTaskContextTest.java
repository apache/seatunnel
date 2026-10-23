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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.source.reader.fetch.Db2SourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.Db2ConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils.Db2Utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Covers optional event metadata and independent connection cleanup through the real fetch context.
 */
class Db2SourceFetchTaskContextTest {

    @Test
    void shouldIgnoreAbsentSourceStructWithNonNullTableId() {
        Struct value = envelope(null);
        TableId table = new TableId("", "DB2INST1", "CUSTOMERS");
        Db2SourceFetchTaskContext.Db2EventMetadataProvider provider = provider();

        Assertions.assertNull(provider.getEventTimestamp(table, null, null, value));
        Assertions.assertNull(provider.getEventSourcePosition(table, null, null, value));
        Assertions.assertNull(provider.getTransactionId(table, null, null, value));
        Assertions.assertNull(provider.getEventTimestamp(table, null, null, null));
        Assertions.assertNull(provider.getEventSourcePosition(table, null, null, null));
        Assertions.assertNull(provider.getTransactionId(table, null, null, null));
    }

    @Test
    void shouldReadSourceMetadataIndependentlyOfCollectionId() {
        Struct source =
                new Struct(sourceSchema())
                        .put(SourceInfo.TIMESTAMP_KEY, 1234L)
                        .put(SourceInfo.COMMIT_LSN_KEY, "commit")
                        .put(SourceInfo.CHANGE_LSN_KEY, "change");
        Struct value = envelope(source);
        Db2SourceFetchTaskContext.Db2EventMetadataProvider provider = provider();
        Map<String, String> expected = new HashMap<>();
        expected.put(SourceInfo.COMMIT_LSN_KEY, "commit");
        expected.put(SourceInfo.CHANGE_LSN_KEY, "change");

        Assertions.assertEquals(
                Instant.ofEpochMilli(1234L), provider.getEventTimestamp(null, null, null, value));
        Assertions.assertEquals(expected, provider.getEventSourcePosition(null, null, null, value));
        Assertions.assertEquals("commit", provider.getTransactionId(null, null, null, value));
        source.put(SourceInfo.TIMESTAMP_KEY, null);
        Assertions.assertNull(provider.getEventTimestamp(null, null, null, value));
    }

    @Test
    void shouldReleaseMetadataConnectionWhenDataConnectionCloseFails() throws Exception {
        Db2Connection data = Mockito.mock(Db2Connection.class);
        Db2Connection metadata = Mockito.mock(Db2Connection.class);
        AtomicBoolean metadataClosed = new AtomicBoolean();
        Mockito.doThrow(new SQLException("data close failure")).when(data).close();
        Mockito.doAnswer(
                        invocation -> {
                            metadataClosed.set(true);
                            return null;
                        })
                .when(metadata)
                .close();

        Db2SourceFetchTaskContext context = configureContext(data, metadata);
        Assertions.assertSame(metadata, context.getMetadataConnection());
        context.close();

        Assertions.assertTrue(
                metadataClosed.get(),
                "The auxiliary DB2 connection must be released after primary close fails");
    }

    private static Db2SourceFetchTaskContext configureContext(
            Db2Connection data, Db2Connection metadata) {
        Db2SourceConfigFactory factory = new Db2SourceConfigFactory();
        factory.hostname("localhost")
                .port(50000)
                .username("user")
                .password("password")
                .databaseList("E2E");
        Db2SourceConfig config = factory.create(0);
        JdbcDataSourceDialect dialect = Mockito.mock(JdbcDataSourceDialect.class);
        Db2DatabaseSchema schema = Mockito.mock(Db2DatabaseSchema.class);
        try (MockedStatic<Db2ConnectionUtils> connections =
                        Mockito.mockStatic(Db2ConnectionUtils.class);
                MockedStatic<Db2Utils> schemas = Mockito.mockStatic(Db2Utils.class)) {
            connections
                    .when(
                            () ->
                                    Db2ConnectionUtils.createDb2Connection(
                                            Mockito.any(Configuration.class)))
                    .thenReturn(data, metadata);
            schemas.when(
                            () ->
                                    Db2Utils.createDb2DatabaseSchema(
                                            Mockito.any(Db2ConnectorConfig.class),
                                            Mockito.same(data)))
                    .thenReturn(schema);
            Db2SourceFetchTaskContext context = new Db2SourceFetchTaskContext(config, dialect);
            context.configure(
                    new IncrementalSplit(
                            "stream",
                            Collections.emptyList(),
                            LsnOffset.INITIAL_OFFSET,
                            LsnOffset.NO_STOPPING_OFFSET,
                            Collections.emptyList()));
            EmbeddedDatabaseHistory.removeHistory(
                    config.getDbzConfiguration()
                            .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME));
            return context;
        }
    }

    private static Db2SourceFetchTaskContext.Db2EventMetadataProvider provider() {
        return new Db2SourceFetchTaskContext.Db2EventMetadataProvider();
    }

    private static Struct envelope(Struct source) {
        return new Struct(
                        SchemaBuilder.struct()
                                .field(Envelope.FieldName.SOURCE, sourceSchema())
                                .build())
                .put(Envelope.FieldName.SOURCE, source);
    }

    private static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .optional()
                .field(SourceInfo.TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.COMMIT_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }
}
