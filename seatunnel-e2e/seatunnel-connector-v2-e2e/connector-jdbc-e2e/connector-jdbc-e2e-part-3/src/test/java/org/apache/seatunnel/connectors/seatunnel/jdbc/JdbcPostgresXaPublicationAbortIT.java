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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableCommitInfo;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableState;
import org.apache.seatunnel.api.sink.multitablesink.SinkIdentifier;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.event.CloseTableEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcExactlyOnceSinkWriter;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.flow.SinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies physical PostgreSQL XA rollback when Zeta cannot publish a prepared checkpoint. */
class JdbcPostgresXaPublicationAbortIT {

    @Test
    void rollsBackPreparedXaWithoutPublishingOrAcknowledgingBarrier() throws Exception {
        try (PostgreSQLContainer<?> postgres =
                new PostgreSQLContainer<>("postgres:14-alpine")
                        .withCommand("postgres", "-c", "max_prepared_transactions=10")) {
            postgres.start();
            try (Connection db =
                            DriverManager.getConnection(
                                    postgres.getJdbcUrl(),
                                    postgres.getUsername(),
                                    postgres.getPassword());
                    Statement observer = db.createStatement()) {
                observer.execute("create table sink_xa(id int primary key)");
                assertFailedPublicationRollsBack(postgres, observer);
            }
        }
    }

    private static void assertFailedPublicationRollsBack(
            PostgreSQLContainer<?> postgres, Statement observer) throws Exception {
        TablePath table = TablePath.of(postgres.getDatabaseName(), "public", "sink_xa");
        SinkIdentifier id = SinkIdentifier.of(table.getFullName(), 0);
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.INT_TYPE)
                                                .sourceType("INTEGER")
                                                .build()))
                        .build();
        Map<String, Object> config = new HashMap<>();
        config.put("url", postgres.getJdbcUrl());
        config.put("user", postgres.getUsername());
        config.put("password", postgres.getPassword());
        config.put("driver", "org.postgresql.Driver");
        config.put("database", "public");
        config.put("schema", "public");
        config.put("table", "sink_xa");
        config.put("generate_sink_sql", true);
        config.put("primary_keys", Collections.singletonList("id"));
        config.put("is_exactly_once", true);
        config.put("xa_data_source_class_name", "org.postgresql.xa.PGXADataSource");
        SeaTunnelSink<SeaTunnelRow, MultiTableState, MultiTableCommitInfo, String> sink =
                Mockito.mock(SeaTunnelSink.class);
        Mockito.when(sink.createWriter(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            SinkWriter.Context context = invocation.getArgument(0);
                            JdbcExactlyOnceSinkWriter child =
                                    new JdbcExactlyOnceSinkWriter(
                                            table,
                                            context,
                                            new JobContext(11L),
                                            new PostgresDialect(),
                                            JdbcSinkConfig.of(ReadonlyConfig.fromMap(config)),
                                            schema,
                                            schema,
                                            Collections.emptyList());
                            return new MultiTableSinkWriter(
                                    Collections.singletonMap(id, child),
                                    1,
                                    Collections.singletonMap(id, context));
                        });
        Serializer<MultiTableCommitInfo> serializer = Mockito.mock(Serializer.class);
        IOException failure = new IOException("injected failure serializing prepared commit info");
        AtomicInteger entered = new AtomicInteger();
        Mockito.when(serializer.serialize(Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            assertEquals(
                                    1, count(observer, "select count(*) from pg_prepared_xacts"));
                            assertEquals(0, count(observer, "select count(*) from sink_xa"));
                            entered.incrementAndGet();
                            throw failure;
                        });
        Mockito.when(sink.getCommitInfoSerializer()).thenReturn(Optional.of(serializer));
        SinkAction<SeaTunnelRow, MultiTableState, MultiTableCommitInfo, String> action =
                new SinkAction<>(7L, "sink", sink, Collections.emptySet(), Collections.emptySet());
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        TaskExecutionContext execution = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(task.getExecutionContext()).thenReturn(execution);
        Mockito.when(execution.getTaskExecutionService())
                .thenReturn(Mockito.mock(TaskExecutionService.class));
        Mockito.when(task.getEnvOptions()).thenReturn(Collections.emptyMap());
        TaskLocation location = new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 2L, 0);
        SinkFlowLifeCycle<SeaTunnelRow, MultiTableCommitInfo, String, MultiTableState> flow =
                new SinkFlowLifeCycle<>(
                        action,
                        location,
                        0,
                        task,
                        location,
                        true,
                        new CompletableFuture<>(),
                        new SeaTunnelMetricsContext());
        flow.init();
        flow.restoreState(Collections.emptyList());

        Barrier barrier = Mockito.mock(Barrier.class);
        Mockito.when(barrier.snapshot()).thenReturn(true);
        Mockito.when(barrier.getId()).thenReturn(42L);
        try {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {11});
            row.setTableId(table.getFullName());
            flow.received(new Record<>(row));
            flow.received(new Record<>(new CloseTableEvent(table, 0, 1)));
            RuntimeException thrown =
                    assertThrows(
                            RuntimeException.class, () -> flow.received(new Record<>(barrier)));
            assertSame(failure, thrown.getCause());
            assertEquals(1, entered.get(), "serialization must fail after physical XA prepare");
            assertEquals(
                    0,
                    count(observer, "select count(*) from pg_prepared_xacts"),
                    "checkpoint publication failure left a prepared XA transaction");
            assertEquals(0, count(observer, "select count(*) from sink_xa"));
            Mockito.verify(execution, Mockito.never()).sendToMember(Mockito.any(), Mockito.any());
            Mockito.verify(task, Mockito.never()).ack(barrier);
        } finally {
            flow.close();
        }
    }

    private static int count(Statement observer, String sql) throws Exception {
        try (ResultSet result = observer.executeQuery(sql)) {
            result.next();
            return result.getInt(1);
        }
    }
}
