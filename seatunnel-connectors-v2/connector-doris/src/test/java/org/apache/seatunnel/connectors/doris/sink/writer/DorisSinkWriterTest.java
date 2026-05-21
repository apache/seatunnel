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

package org.apache.seatunnel.connectors.doris.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.rest.models.RespContent;
import org.apache.seatunnel.connectors.doris.sink.LoadStatus;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DorisSinkWriterTest {

    @Test
    void testInitializeLoadUsesFrontendWhenDirectToBeDisabled() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        when(frontendLoad.stopLoad()).thenReturn(successRespContent(11L));

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(false, false),
                            "job_1",
                            factory);

            Assertions.assertEquals(
                    Collections.singletonList("fe1:8030"), factory.getCreatedHosts());
            verify(frontendLoad, times(1)).startLoad(anyString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testDirectToBeWithoutTwoPhaseCommitUsesBackendOnly() throws Exception {
        DorisStreamLoad backendLoad = mock(DorisStreamLoad.class);
        when(backendLoad.stopLoad()).thenReturn(successRespContent(12L));

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("be1:8040", backendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(true, false),
                            "job_1",
                            factory);

            Assertions.assertEquals(
                    Collections.singletonList("be1:8040"), factory.getCreatedHosts());
            verify(backendLoad, times(1)).startLoad(anyString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testDirectToBeUsesBackendForWriteButFrontendForTwoPhaseCommit() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        DorisStreamLoad backendLoad = mock(DorisStreamLoad.class);
        when(backendLoad.stopLoad()).thenReturn(successRespContent(22L));
        when(backendLoad.getDb()).thenReturn("test_db");
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);
        factory.register("be1:8040", backendLoad);

        DorisSinkWriter writer = null;
        try {
            writer =
                    new DorisSinkWriter(
                            new DefaultSinkWriterContext(0, 1),
                            new ArrayList<>(),
                            mockCatalogTable(),
                            createSinkConfig(true, true),
                            "job_1",
                            factory);

            Assertions.assertEquals(
                    Arrays.asList("fe1:8030", "be1:8040"), factory.getCreatedHosts());
            verify(frontendLoad, times(1)).abortPreCommit(anyString(), anyLong());
            verify(backendLoad, times(1)).startLoad(anyString());

            Optional<DorisCommitInfo> commitInfo = writer.prepareCommit();
            Assertions.assertTrue(commitInfo.isPresent());
            Assertions.assertEquals("fe1:8030", commitInfo.get().getHostPort());
            Assertions.assertEquals("test_db", commitInfo.get().getDb());

            writer.abortPrepare();
            verify(frontendLoad, times(2)).abortPreCommit(anyString(), anyLong());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void testDirectToBeClosesControlStreamLoadWhenBackendInitializationFails() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doNothing().when(frontendLoad).abortPreCommit(anyString(), anyLong());

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);
        factory.registerFailure("be1:8040", new IllegalStateException("be init failed"));

        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        new DorisSinkWriter(
                                new DefaultSinkWriterContext(0, 1),
                                new ArrayList<>(),
                                mockCatalogTable(),
                                createSinkConfig(true, true),
                                "job_1",
                                factory));

        verify(frontendLoad, times(1)).close();
    }

    @Test
    void testInitializeLoadClosesFailedStreamLoadWhenAbortPreCommitFails() throws Exception {
        DorisStreamLoad frontendLoad = mock(DorisStreamLoad.class);
        doThrow(new IllegalStateException("abort failed"))
                .when(frontendLoad)
                .abortPreCommit(anyString(), anyLong());

        RecordingStreamLoadFactory factory = new RecordingStreamLoadFactory();
        factory.register("fe1:8030", frontendLoad);

        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        new DorisSinkWriter(
                                new DefaultSinkWriterContext(0, 1),
                                new ArrayList<>(),
                                mockCatalogTable(),
                                createSinkConfig(true, true),
                                "job_1",
                                factory));

        verify(frontendLoad, times(1)).close();
    }

    private DorisSinkConfig createSinkConfig(boolean directToBe, boolean enable2PC) {
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "fe1:8030");
        options.put("benodes", "be1:8040");
        options.put("direct_to_be", directToBe);
        options.put("sink.enable-2pc", enable2PC);
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("sink.label-prefix", "test_job");
        options.put("doris.config", createStreamLoadProperties());
        return DorisSinkConfig.of(ReadonlyConfig.fromMap(options));
    }

    private Map<String, String> createStreamLoadProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("format", "json");
        properties.put("read_json_by_line", "true");
        return properties;
    }

    private CatalogTable mockCatalogTable() {
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTablePath()).thenReturn(TablePath.of("test_db", "test_table"));
        when(catalogTable.getTableSchema()).thenReturn(mock(TableSchema.class));
        when(catalogTable.getSeaTunnelRowType())
                .thenReturn(
                        new SeaTunnelRowType(
                                new String[] {"id"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    BasicType.LONG_TYPE
                                }));
        return catalogTable;
    }

    private RespContent successRespContent(long txnId) {
        RespContent respContent = new RespContent();
        respContent.setTxnId(txnId);
        respContent.setStatus(LoadStatus.SUCCESS);
        return respContent;
    }

    private static final class RecordingStreamLoadFactory implements DorisStreamLoadFactory {
        private final Map<String, DorisStreamLoad> streamLoads = new HashMap<>();
        private final Map<String, RuntimeException> createFailures = new HashMap<>();
        private final List<String> createdHosts = new ArrayList<>();

        private void register(String host, DorisStreamLoad streamLoad) {
            streamLoads.put(host, streamLoad);
        }

        private void registerFailure(String host, RuntimeException exception) {
            createFailures.put(host, exception);
        }

        @Override
        public DorisStreamLoad create(
                String hostPort,
                TablePath tablePath,
                DorisSinkConfig dorisSinkConfig,
                LabelGenerator labelGenerator) {
            createdHosts.add(hostPort);
            RuntimeException createFailure = createFailures.get(hostPort);
            if (createFailure != null) {
                throw createFailure;
            }
            DorisStreamLoad streamLoad = streamLoads.get(hostPort);
            if (streamLoad == null) {
                throw new IllegalArgumentException("Unexpected host " + hostPort);
            }
            return streamLoad;
        }

        private List<String> getCreatedHosts() {
            return createdHosts;
        }
    }
}
