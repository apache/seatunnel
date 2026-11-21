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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.hudi.catalog.HudiCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.HudiClientManager;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.HudiSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.WriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiAuthenticationContext;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HudiSinkWriterTest {

    @Test
    void shouldDeclareAndRetainKerberosOptionsWithoutLeakingThemInConfigText() {
        java.util.Map<String, Object> options = new java.util.HashMap<>();
        options.put("table_dfs_path", "/tmp/hudi");
        options.put("table_name", "orders");
        options.put("kerberos_principal", "sensitive-principal");
        options.put("kerberos_keytab_path", "sensitive-keytab");
        options.put("krb5_path", "sensitive-krb5");
        HudiSinkConfig config = HudiSinkConfig.of(ReadonlyConfig.fromMap(options));
        Assertions.assertEquals("sensitive-principal", config.getKerberosPrincipal());
        Assertions.assertEquals("sensitive-keytab", config.getKerberosKeytabPath());
        Assertions.assertEquals("sensitive-krb5", config.getKrb5Path());
        Assertions.assertFalse(config.toString().contains("sensitive-"));
        for (org.apache.seatunnel.api.configuration.Option<?> option :
                java.util.Arrays.asList(
                        HudiSinkOptions.KERBEROS_PRINCIPAL,
                        HudiSinkOptions.KERBEROS_KEYTAB_PATH,
                        HudiSinkOptions.KRB5_PATH)) {
            Assertions.assertTrue(
                    new HudiSinkFactory().optionRule().getOptionalOptions().contains(option));
            Assertions.assertTrue(
                    new HudiCatalogFactory().optionRule().getOptionalOptions().contains(option));
        }
    }

    @Test
    void shouldScopeDirectAndCachedClientLifecycleIncludingWritesAndDeletes() throws Exception {
        for (boolean cached : new boolean[] {false, true}) {
            verifyClientIdentity(cached);
        }
    }

    private void verifyClientIdentity(boolean cached) throws Exception {
        UserGroupInformation user = UserGroupInformation.createRemoteUser("owned-client");
        Configuration conf = new Configuration(false);
        HudiSinkConfig config = HudiSinkConfig.builder().build();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        HudiTableConfig table =
                HudiTableConfig.builder()
                        .tableName("orders")
                        .recordKeyFields("id")
                        .opType(WriteOperationType.UPSERT)
                        .batchSize(1)
                        .build();
        HoodieJavaWriteClient<HoodieAvroPayload> client = mock(HoodieJavaWriteClient.class);
        AtomicInteger operations = new AtomicInteger();
        Mockito.doAnswer(
                        i -> {
                            Assertions.assertEquals(
                                    "owned-client",
                                    UserGroupInformation.getCurrentUser().getUserName());
                            operations.incrementAndGet();
                            return "001";
                        })
                .when(client)
                .startCommit();
        Mockito.doAnswer(
                        i -> {
                            Assertions.assertEquals(
                                    "owned-client",
                                    UserGroupInformation.getCurrentUser().getUserName());
                            operations.incrementAndGet();
                            return java.util.Collections.emptyList();
                        })
                .when(client)
                .upsert(Mockito.anyList(), Mockito.anyString());
        Mockito.doAnswer(
                        i -> {
                            Assertions.assertEquals(
                                    "owned-client",
                                    UserGroupInformation.getCurrentUser().getUserName());
                            operations.incrementAndGet();
                            return java.util.Collections.emptyList();
                        })
                .when(client)
                .delete(Mockito.anyList(), Mockito.anyString());
        Mockito.doAnswer(
                        i -> {
                            Assertions.assertEquals(
                                    "owned-client",
                                    UserGroupInformation.getCurrentUser().getUserName());
                            operations.incrementAndGet();
                            return null;
                        })
                .when(client)
                .close();

        try (MockedConstruction<HudiAuthenticationContext> contexts =
                        Mockito.mockConstruction(
                                HudiAuthenticationContext.class,
                                (auth, context) -> {
                                    when(auth.getConfiguration()).thenReturn(conf);
                                    Mockito.doAnswer(
                                                    i ->
                                                            user.doAs(
                                                                    (PrivilegedExceptionAction<?>)
                                                                            i.getArgument(0)))
                                            .when(auth)
                                            .execute(Mockito.any());
                                    Mockito.doAnswer(
                                                    i ->
                                                            user.doAs(
                                                                    (PrivilegedExceptionAction<?>)
                                                                            i.getArgument(0)))
                                            .when(auth)
                                            .close(Mockito.any());
                                });
                MockedStatic<HudiUtil> factory = Mockito.mockStatic(HudiUtil.class)) {
            factory.when(
                            () ->
                                    HudiUtil.createHoodieJavaWriteClient(
                                            config, rowType, "orders", conf))
                    .thenAnswer(
                            i -> {
                                Assertions.assertEquals(
                                        "owned-client",
                                        UserGroupInformation.getCurrentUser().getUserName());
                                return client;
                            });
            HudiClientManager manager = new HudiClientManager(config);
            WriteClientProvider provider =
                    cached
                            ? new HudiWriteClientProviderProxy(manager, rowType, 0, "orders")
                            : new HudiWriteClientProvider(config, "orders", rowType);
            HudiRecordWriter writer = new HudiRecordWriter(table, provider, rowType);
            writer.open();
            writer.writeRecord(new SeaTunnelRow(new Object[] {1}));
            SeaTunnelRow deleted = new SeaTunnelRow(new Object[] {1});
            deleted.setRowKind(org.apache.seatunnel.api.table.type.RowKind.DELETE);
            writer.writeRecord(deleted);
            writer.close();
            Assertions.assertEquals(5, operations.get());
            Assertions.assertEquals(1, contexts.constructed().size());
            factory.verify(
                    () -> HudiUtil.createHoodieJavaWriteClient(config, rowType, "orders", conf));
            if (cached) {
                Assertions.assertFalse(manager.containsClient("orders", 0));
            }
        }
    }

    @Test
    void shouldRegisterAndExecuteTimerFlush() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<HudiRecordWriter> recordWriters = createWriter(context)) {
            verify(context, times(1)).registerFlushAction(actionCaptor.capture());

            actionCaptor.getValue().run();

            verify(recordWriters.constructed().get(0), times(1)).flush();
        }
    }

    @Test
    void shouldPropagateTimerFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<HudiRecordWriter> recordWriters = createWriter(context)) {
            HudiConnectorException expected =
                    new HudiConnectorException(
                            HudiErrorCode.FLUSH_DATA_FAILED, "timer flush failed");
            doThrow(expected).when(recordWriters.constructed().get(0)).flush();
            verify(context).registerFlushAction(actionCaptor.capture());

            HudiConnectorException actual =
                    Assertions.assertThrows(
                            HudiConnectorException.class, actionCaptor.getValue()::run);

            Assertions.assertSame(expected, actual);
        }
    }

    @Test
    void shouldFlushCurrentRecordWriterAfterResourceManagerReplacement() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);
        MultiTableResourceManager<HudiClientManager> resourceManager =
                mock(MultiTableResourceManager.class);
        when(resourceManager.getSharedResource())
                .thenReturn(Optional.of(mock(HudiClientManager.class)));

        try (MockedConstruction<HudiRecordWriter> recordWriters =
                Mockito.mockConstruction(HudiRecordWriter.class)) {
            HudiSinkWriter writer =
                    new HudiSinkWriter(
                            context,
                            mock(SeaTunnelRowType.class),
                            mock(HudiSinkConfig.class),
                            mock(HudiTableConfig.class));
            verify(context).registerFlushAction(actionCaptor.capture());

            writer.setMultiTableResourceManager(resourceManager, 0);
            actionCaptor.getValue().run();

            Assertions.assertEquals(2, recordWriters.constructed().size());
            verify(recordWriters.constructed().get(0), never()).flush();
            verify(recordWriters.constructed().get(1)).flush();
        }
    }

    private MockedConstruction<HudiRecordWriter> createWriter(SinkWriter.Context context) {
        MockedConstruction<HudiRecordWriter> recordWriters =
                Mockito.mockConstruction(HudiRecordWriter.class);
        new HudiSinkWriter(
                context,
                mock(SeaTunnelRowType.class),
                mock(HudiSinkConfig.class),
                mock(HudiTableConfig.class));
        return recordWriters;
    }
}
