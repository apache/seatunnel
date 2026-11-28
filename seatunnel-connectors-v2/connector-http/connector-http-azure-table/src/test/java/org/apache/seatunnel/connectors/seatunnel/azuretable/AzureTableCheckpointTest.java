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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Guards Azure Table signing, keyset serialization and coordinator checkpoint scope.
 *
 * <p>These tests require no service socket. The dedicated engine module exercises live request and
 * restore behavior against its independent HTTPS peer.
 */
class AzureTableCheckpointTest {
    private static final String KEY =
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWYwMTIzNDU2Nzg5YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZg==";

    @Test
    void signsDiscoveryAndEntityPathsWithTheTableSharedKeyFormat() {
        AzureTableConfig config = new AzureTableConfig(ReadonlyConfig.fromMap(settings()));
        String date = "Fri, 01 Mar 2024 12:34:56 GMT";
        assertEquals(
                "SharedKey fixturetableaccount:Zr7i9fx7nv5CpW/ouAVwPvnWH0WpChwV9SQvfdKRU0s=",
                AzureTableHttp.authorization(config, "/Tables", date));
        assertEquals(
                "SharedKey fixturetableaccount:yFCqA1yRq+DkXJsycT4jVylJNAuuDf2GF7WulBcTlo4=",
                AzureTableHttp.authorization(config, "/Orders()", date));
    }

    @Test
    void keepsEscapedCompositeKeysSeparateFromOpaqueContinuationAndAcceptsEmptyKeys() {
        Map<String, Object> options = settings();
        options.put("tables", List.of("Orders"));
        options.put("start_after_partition_keys", Map.of("orders", "West'冬&+%"));
        options.put("start_after_row_keys", Map.of("ORDERS", ""));
        AzureTableConfig config = new AzureTableConfig(ReadonlyConfig.fromMap(options));
        try (AzureTableHttp transport = new AzureTableHttp(config)) {
            AzureTableApi api = new AzureTableApi(config, transport);
            String query = api.entityQuery("Orders", new AzureTableApi.Cursor("next+/=&%", null));
            Map<String, String> parameters = new HashMap<>();
            for (String field : query.substring(1).split("&")) {
                String[] pair = field.split("=", 2);
                parameters.put(pair[0], URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
            }
            assertEquals(
                    Map.of(
                            "$top",
                            "1000",
                            "$filter",
                            "(PartitionKey gt 'West''冬&+%') or (PartitionKey eq 'West''冬&+%' and RowKey gt '')",
                            "NextPartitionKey",
                            "next+/=&%"),
                    parameters);
        }
        assertTrue(AzureTableConfig.validKey(""));
        assertFalse(AzureTableConfig.validKey("not/a/key"));
        assertFalse(AzureTableConfig.validTable("Tables"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void restoresPendingDiscoveryWithoutSerializingTheKeyAndRejectsAnotherCloud() throws Exception {
        SourceSplitEnumerator.Context<AzureTableSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        AzureTableSource source = new AzureTableSource(ReadonlyConfig.fromMap(settings()));
        AzureTableSplit initial = source.createEnumerator(context).snapshotState(1);
        AzureTableSplit saved;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(initial);
            output.flush();
            String wire = bytes.toString(StandardCharsets.ISO_8859_1);
            assertFalse(wire.contains(KEY));
            assertFalse(wire.contains("0123456789abcdef"));
            try (ObjectInputStream input =
                    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                saved = (AzureTableSplit) input.readObject();
            }
        }
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        Map<String, Object> rotated = settings();
        rotated.put("secret_key", "c3ludGhldGljIGtleSByb3RhdGlvbg==");
        AzureTableSource renewed = new AzureTableSource(ReadonlyConfig.fromMap(rotated));
        renewed.restoreEnumerator(context, saved).run();
        ArgumentCaptor<AzureTableSplit> active = ArgumentCaptor.forClass(AzureTableSplit.class);
        verify(context).assignSplit(eq(0), active.capture());
        verify(context).signalNoMoreSplits(1);
        assertTrue(active.getValue().discovering);
        assertNull(active.getValue().cursor);
        rotated.put("endpoint_suffix", "core.chinacloudapi.cn");
        AzureTableSource china = new AzureTableSource(ReadonlyConfig.fromMap(rotated));
        assertThrows(IllegalStateException.class, () -> china.restoreEnumerator(context, saved));
    }

    @Test
    void detachesDiscoveryCheckpointAndContinuesAcrossEmptyPagesWithoutReordering() {
        AzureTableConfig config = new AzureTableConfig(ReadonlyConfig.fromMap(settings()));
        AzureTableSplit progress = config.newSplit();
        progress.commitDiscovery(
                new AzureTableApi.DiscoveryPage(
                        List.of("Orders"), new AzureTableApi.Cursor("one", null)),
                config);
        AzureTableSplit saved = progress.copy();
        progress.commitDiscovery(
                new AzureTableApi.DiscoveryPage(List.of(), new AzureTableApi.Cursor("two", null)),
                config);
        progress.commitDiscovery(
                new AzureTableApi.DiscoveryPage(List.of("Customers"), null), config);
        assertTrue(saved.discovering);
        assertEquals(List.of("Orders"), saved.tables);
        assertEquals("one", saved.cursor.first);
        assertEquals(1, saved.page);
        assertFalse(progress.discovering);
        assertEquals(List.of("Orders", "Customers"), progress.tables);
        assertEquals(0, progress.page);
        assertNull(progress.cursor);
    }

    @Test
    void rejectsArbitrarySigningHostsAndUnpairedKeyRangesBeforeNetworkWork() {
        Map<String, Object> options = settings();
        options.put("endpoint_suffix", "core.windows.net.example.invalid");
        assertThrows(
                IllegalArgumentException.class,
                () -> new AzureTableConfig(ReadonlyConfig.fromMap(options)));
        options.put("endpoint_suffix", "core.usgovcloudapi.net");
        assertEquals(
                "fixturetableaccount.table.core.usgovcloudapi.net",
                new AzureTableConfig(ReadonlyConfig.fromMap(options)).endpoint.getHost());
        options.put("tables", List.of("Orders"));
        options.put("start_after_partition_keys", Map.of("Orders", "prior"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new AzureTableConfig(ReadonlyConfig.fromMap(options)));
    }

    private static Map<String, Object> settings() {
        return new HashMap<>(Map.of("account_name", "fixturetableaccount", "secret_key", KEY));
    }
}
