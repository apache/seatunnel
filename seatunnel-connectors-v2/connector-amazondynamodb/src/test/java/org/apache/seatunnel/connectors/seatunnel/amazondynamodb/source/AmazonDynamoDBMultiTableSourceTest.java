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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class AmazonDynamoDBMultiTableSourceTest {

    private static final String CONNECTION =
            "url = \"http://localhost:8000\"\n"
                    + "region = \"us-east-1\"\n"
                    + "access_key_id = \"dummy-key\"\n"
                    + "secret_access_key = \"dummy-secret\"\n";

    private static final String ORDERS =
            "{ table = orders, parallel_scan_threads = 2, scan_item_limit = 10,"
                    + " schema { fields { id = string, amount = int } } }";

    private static final String CUSTOMERS =
            "{ table = customers,"
                    + " schema { table = \"crm.customer_view\", fields { id = string, name = string, vip = boolean } } }";

    private static final String SINGLE_TABLE =
            "table = orders\nschema { fields { id = string, amount = int } }\n";

    /**
     * {@link AmazonDynamoDBSourceState} serialized by the single-table source before splits carried
     * a table identity: segments 0 and 1 of 2 with a scan limit of 5, pending for readers 0 and 1.
     */
    private static final String LEGACY_STATE =
            "rO0ABXNyAFlvcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5hbWF6b25keW5h"
                    + "bW9kYi5zb3VyY2UuQW1hem9uRHluYW1vREJTb3VyY2VTdGF0ZYhyTqknexWFAgADSQALYXNzaWduQ291"
                    + "bnRaAA9zaG91bGRFbnVtZXJhdGVMAA1wZW5kaW5nU3BsaXRzdAAPTGphdmEvdXRpbC9NYXA7eHAAAAAC"
                    + "AHNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhw"
                    + "P0AAAAAAAAx3CAAAABAAAAACc3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhy"
                    + "ABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAABzcgATamF2YS51dGlsLkFycmF5TGlzdHiB"
                    + "0h2Zx2GdAwABSQAEc2l6ZXhwAAAAAXcEAAAAAXNyAFlvcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0"
                    + "b3JzLnNlYXR1bm5lbC5hbWF6b25keW5hbW9kYi5zb3VyY2UuQW1hem9uRHluYW1vREJTb3VyY2VTcGxp"
                    + "dLiOH5Gj9p5OAgADTAAJaXRlbUNvdW50dAATTGphdmEvbGFuZy9JbnRlZ2VyO0wAB3NwbGl0SWRxAH4A"
                    + "C0wADXRvdGFsU2VnbWVudHNxAH4AC3hwc3EAfgAFAAAABXEAfgAHc3EAfgAFAAAAAnhzcQB+AAUAAAAB"
                    + "c3EAfgAIAAAAAXcEAAAAAXNxAH4ACnEAfgANcQB+AA9xAH4ADnh4";

    @Test
    void singleTableConfigKeepsOneTableAndLegacySplits() throws Exception {
        AmazonDynamoDBSource source = source(SINGLE_TABLE + "parallel_scan_threads = 3\n");

        Assertions.assertEquals(1, source.getProducedCatalogTables().size());
        List<AmazonDynamoDBSourceSplit> splits = enumerate(source);
        Assertions.assertEquals(3, splits.size());
        for (AmazonDynamoDBSourceSplit split : splits) {
            Assertions.assertNull(split.getTableId());
            Assertions.assertEquals(split.getSplitId().toString(), split.splitId());
            Assertions.assertEquals(3, split.getTotalSegments());
            Assertions.assertEquals(1, split.getItemCount());
        }
    }

    @Test
    void producesOneCatalogTablePerConfiguredTable() {
        AmazonDynamoDBSource source = source("tables_configs = [" + ORDERS + "," + CUSTOMERS + "]");

        List<CatalogTable> tables = source.getProducedCatalogTables();
        Assertions.assertEquals(
                Arrays.asList("orders", "crm.customer_view"),
                tables.stream()
                        .map(table -> table.getTableId().toTablePath().toString())
                        .collect(Collectors.toList()));
        Assertions.assertArrayEquals(
                new String[] {"id", "amount"}, tables.get(0).getSeaTunnelRowType().getFieldNames());
        Assertions.assertArrayEquals(
                new String[] {"id", "name", "vip"},
                tables.get(1).getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void enumeratesParallelScanSegmentsPerTable() throws Exception {
        AmazonDynamoDBSource source =
                source(
                        "parallel_scan_threads = 3\nscan_item_limit = 7\n"
                                + "tables_configs = ["
                                + ORDERS
                                + ","
                                + CUSTOMERS
                                + "]");

        List<AmazonDynamoDBSourceSplit> splits = enumerate(source);

        Map<String, List<AmazonDynamoDBSourceSplit>> byTable =
                splits.stream()
                        .collect(Collectors.groupingBy(AmazonDynamoDBSourceSplit::getTableId));
        Assertions.assertEquals(2, byTable.get("orders").size());
        Assertions.assertEquals(3, byTable.get("crm.customer_view").size());
        for (AmazonDynamoDBSourceSplit split : byTable.get("orders")) {
            Assertions.assertEquals(2, split.getTotalSegments());
            Assertions.assertEquals(10, split.getItemCount());
        }
        for (AmazonDynamoDBSourceSplit split : byTable.get("crm.customer_view")) {
            Assertions.assertEquals(3, split.getTotalSegments());
            Assertions.assertEquals(7, split.getItemCount());
        }
        Assertions.assertEquals(
                Arrays.asList(
                        "crm.customer_view:0",
                        "crm.customer_view:1",
                        "crm.customer_view:2",
                        "orders:0",
                        "orders:1"),
                splits.stream()
                        .map(AmazonDynamoDBSourceSplit::splitId)
                        .sorted()
                        .collect(Collectors.toList()));
    }

    @Test
    void readerRoutesRowsToTheirTable() throws Exception {
        AmazonDynamoDBSource source = source("tables_configs = [" + ORDERS + "," + CUSTOMERS + "]");
        List<AmazonDynamoDBSourceSplit> splits = roundTrip(enumerate(source));
        Map<String, List<Map<String, AttributeValue>>> data = new HashMap<>();
        data.put(
                "orders",
                Arrays.asList(
                        item("id", s("o1"), "amount", n("10")),
                        item("id", s("o2"), "amount", n("20"))));
        data.put(
                "customers",
                Collections.singletonList(
                        item(
                                "id",
                                s("c1"),
                                "name",
                                s("Ada"),
                                "vip",
                                AttributeValue.builder().bool(true).build())));
        DynamoDbClient client = client(data);

        List<SeaTunnelRow> rows = read(source, client, splits);

        Assertions.assertEquals(
                Arrays.asList("[o1, 10]", "[o2, 20]"),
                rows.stream()
                        .filter(row -> "orders".equals(row.getTableId()))
                        .map(row -> Arrays.toString(row.getFields()))
                        .sorted()
                        .collect(Collectors.toList()));
        Assertions.assertEquals(
                Collections.singletonList("[c1, Ada, true]"),
                rows.stream()
                        .filter(row -> "crm.customer_view".equals(row.getTableId()))
                        .map(row -> Arrays.toString(row.getFields()))
                        .collect(Collectors.toList()));
        Assertions.assertEquals(
                0,
                rows.stream()
                        .filter(
                                row ->
                                        !"orders".equals(row.getTableId())
                                                && !"crm.customer_view".equals(row.getTableId()))
                        .count());

        ArgumentCaptor<ScanRequest> requests = ArgumentCaptor.forClass(ScanRequest.class);
        Mockito.verify(client, Mockito.atLeastOnce()).scanPaginator(requests.capture());
        Map<String, List<ScanRequest>> requestsByTable =
                requests.getAllValues().stream()
                        .collect(Collectors.groupingBy(ScanRequest::tableName));
        Assertions.assertEquals(2, requestsByTable.get("orders").size());
        Assertions.assertEquals(2, requestsByTable.get("customers").size());
        for (ScanRequest request : requestsByTable.get("orders")) {
            Assertions.assertEquals(2, request.totalSegments());
            Assertions.assertEquals(10, request.limit());
        }
    }

    @Test
    void restoresStateWrittenBeforeSplitsCarriedTableIdentity() throws Exception {
        AmazonDynamoDBSourceState state = deserialize(Base64.getDecoder().decode(LEGACY_STATE));
        List<AmazonDynamoDBSourceSplit> legacySplits = new ArrayList<>();
        state.getPendingSplits().values().forEach(legacySplits::addAll);
        Assertions.assertEquals(2, legacySplits.size());
        for (AmazonDynamoDBSourceSplit split : legacySplits) {
            Assertions.assertNull(split.getTableId());
            Assertions.assertEquals(split.getSplitId().toString(), split.splitId());
        }

        AmazonDynamoDBSource source = source(SINGLE_TABLE);
        TestingContext context = new TestingContext();
        SourceSplitEnumerator<AmazonDynamoDBSourceSplit, AmazonDynamoDBSourceState> enumerator =
                source.restoreEnumerator(context, state);
        enumerator.open();
        enumerator.run();
        enumerator.registerReader(0);
        enumerator.registerReader(1);
        Assertions.assertEquals(1, context.assigned.get(0).size());
        Assertions.assertEquals(1, context.assigned.get(1).size());
        Assertions.assertEquals(2, enumerator.snapshotState(1L).getAssignCount());

        DynamoDbClient client =
                client(
                        Collections.singletonMap(
                                "orders",
                                Collections.singletonList(item("id", s("o1"), "amount", n("10")))));
        AmazonDynamoDBSourceSplit firstSegment =
                legacySplits.stream().filter(split -> split.getSplitId() == 0).findFirst().get();
        List<SeaTunnelRow> rows = read(source, client, Collections.singletonList(firstSegment));
        Assertions.assertEquals(1, rows.size());
        // single-table rows keep the default table id, as before
        Assertions.assertEquals(new SeaTunnelRow(0).getTableId(), rows.get(0).getTableId());
        Assertions.assertEquals("[o1, 10]", Arrays.toString(rows.get(0).getFields()));
        ArgumentCaptor<ScanRequest> request = ArgumentCaptor.forClass(ScanRequest.class);
        Mockito.verify(client, Mockito.atLeastOnce()).scanPaginator(request.capture());
        Assertions.assertEquals("orders", request.getValue().tableName());
        Assertions.assertEquals(5, request.getValue().limit());
        Assertions.assertEquals(2, request.getValue().totalSegments());
    }

    @Test
    void readerRejectsSplitsThatDoNotMatchTheConfiguredTables() throws Exception {
        AmazonDynamoDBSource multiTable =
                source("tables_configs = [" + ORDERS + "," + CUSTOMERS + "]");
        DynamoDbClient client = client(Collections.emptyMap());

        SeaTunnelRuntimeException legacySplit =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                read(
                                        multiTable,
                                        client,
                                        Collections.singletonList(
                                                new AmazonDynamoDBSourceSplit(0, 1, 1))));
        Assertions.assertTrue(
                legacySplit.getMessage().contains("Cannot restore a single-table"),
                legacySplit.getMessage());

        SeaTunnelRuntimeException unknownTable =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                read(
                                        source(SINGLE_TABLE),
                                        client,
                                        Collections.singletonList(
                                                new AmazonDynamoDBSourceSplit(0, 1, 1, "orders"))));
        Assertions.assertTrue(
                unknownTable.getMessage().contains("Unknown table identity"),
                unknownTable.getMessage());
    }

    @Test
    void multiTableSplitsSurviveSerialization() throws Exception {
        AmazonDynamoDBSourceSplit split = roundTrip(new AmazonDynamoDBSourceSplit(1, 2, 3, "t"));
        Assertions.assertEquals("t", split.getTableId());
        Assertions.assertEquals("t:1", split.splitId());
    }

    @Test
    void optionRuleAcceptsSingleTableOrTablesConfigs() {
        validate(SINGLE_TABLE);
        validate("tables_configs = [" + ORDERS + "," + CUSTOMERS + "]");
    }

    @Test
    void acceptsUnquotedNumericTableName() {
        AmazonDynamoDBSource source =
                source("tables_configs = [{ table = 2024, schema { fields { id = string } } }]");
        Assertions.assertEquals(
                "2024",
                source.getProducedCatalogTables().get(0).getTableId().toTablePath().toString());
    }

    @Test
    void optionRuleRejectsInvalidTableSelection() {
        assertInvalid("", "exactly one option must be set");
        assertInvalid(
                SINGLE_TABLE + "tables_configs = [" + ORDERS + "]",
                "mutually exclusive, but multiple are set");
        assertInvalid("table = orders\n", "'schema' must be configured when using a root-level");
        assertInvalid("tables_configs = []\n", "is not empty");
        assertInvalid(
                "schema { fields { id = string } }\ntables_configs = [" + ORDERS + "]",
                "root-level 'schema' cannot be used with 'tables_configs'");
    }

    @Test
    void optionRuleRejectsInvalidTableEntries() {
        assertInvalid(
                "tables_configs = [{ schema { fields { id = string } } }]",
                "tables_configs[0]: 'table' must be configured and non-blank");
        assertInvalid(
                "tables_configs = ["
                        + ORDERS
                        + ", { table = \" \", schema { fields { id = string } } }]",
                "tables_configs[1]: 'table' must be configured and non-blank");
        assertInvalid(
                "tables_configs = [{ table = orders }]",
                "tables_configs[0]: 'schema' must be configured and non-empty");
        assertInvalid(
                "tables_configs = [" + ORDERS + "," + ORDERS + "]",
                "tables_configs[1]: duplicate table identity 'orders'");
        assertInvalid(
                "tables_configs = ["
                        + ORDERS
                        + ", { table = orders_archive, schema { table = orders, fields { id = string } } }]",
                "tables_configs[1]: duplicate table identity 'orders'");
        assertInvalid(
                "tables_configs = [{ table = orders, region = \"eu-west-1\", schema { fields { id = string } } }]",
                "tables_configs[0]: 'region' must be configured at source level");
        assertInvalid(
                "tables_configs = [{ table = orders, URL = \"http://other:8000\", schema { fields { id = string } } }]",
                "tables_configs[0]: unsupported table option 'URL'");
        assertInvalid(
                "tables_configs = [{ table = orders, batch_size = 5, schema { fields { id = string } } }]",
                "tables_configs[0]: unsupported table option 'batch_size'");
        assertInvalid(
                "tables_configs = [{ table = orders, parallel_scan_threads = 0, schema { fields { id = string } } }]",
                "tables_configs[0]: 'scan_item_limit' and 'parallel_scan_threads' must be positive");
        assertInvalid(
                "tables_configs = [{ table = orders, scan_item_limit = many, schema { fields { id = string } } }]",
                "tables_configs[0]: invalid table entry");
    }

    private static void validate(String tableConfig) {
        ConfigValidator.of(config(tableConfig))
                .validate(new AmazonDynamoDBSourceFactory().optionRule());
    }

    private static void assertInvalid(String tableConfig, String expectedMessage) {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(tableConfig));
        Assertions.assertTrue(
                exception.getMessage().contains(expectedMessage), exception.getMessage());
        Assertions.assertFalse(exception.getMessage().contains("dummy-secret"));
    }

    private static ReadonlyConfig config(String tableConfig) {
        return ReadonlyConfig.fromConfig(ConfigFactory.parseString(CONNECTION + tableConfig));
    }

    private static AmazonDynamoDBSource source(String tableConfig) {
        ReadonlyConfig config = config(tableConfig);
        AmazonDynamoDBSourceFactory factory = new AmazonDynamoDBSourceFactory();
        ConfigValidator.of(config).validate(factory.optionRule());
        return (AmazonDynamoDBSource)
                factory.<SeaTunnelRow, AmazonDynamoDBSourceSplit, AmazonDynamoDBSourceState>
                                createSource(
                                        new TableSourceFactoryContext(
                                                config,
                                                AmazonDynamoDBMultiTableSourceTest.class
                                                        .getClassLoader()))
                        .createSource();
    }

    private static List<AmazonDynamoDBSourceSplit> enumerate(AmazonDynamoDBSource source)
            throws Exception {
        TestingContext context = new TestingContext();
        SourceSplitEnumerator<AmazonDynamoDBSourceSplit, AmazonDynamoDBSourceState> enumerator =
                source.createEnumerator(context);
        enumerator.open();
        enumerator.run();
        List<AmazonDynamoDBSourceSplit> splits = new ArrayList<>();
        context.assigned.values().forEach(splits::addAll);
        return splits;
    }

    private static List<SeaTunnelRow> read(
            AmazonDynamoDBSource source,
            DynamoDbClient client,
            List<AmazonDynamoDBSourceSplit> splits)
            throws Exception {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        AmazonDynamoDBSourceReader reader =
                (AmazonDynamoDBSourceReader) source.createReader(context);
        reader.dynamoDbClient = client;
        reader.addSplits(splits);
        reader.handleNoMoreSplits();
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        rows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        for (int i = 0; i < splits.size(); i++) {
            reader.pollNext(collector);
        }
        return rows;
    }

    /** Returns each table's items from segment 0 and nothing from the other segments. */
    private static DynamoDbClient client(Map<String, List<Map<String, AttributeValue>>> data) {
        DynamoDbClient client = Mockito.mock(DynamoDbClient.class);
        Mockito.when(client.scanPaginator(Mockito.any(ScanRequest.class)))
                .thenAnswer(
                        invocation ->
                                new ScanIterable(
                                        client, invocation.getArgument(0, ScanRequest.class)));
        Mockito.when(client.scan(Mockito.any(ScanRequest.class)))
                .thenAnswer(
                        invocation -> {
                            ScanRequest request = invocation.getArgument(0, ScanRequest.class);
                            List<Map<String, AttributeValue>> items =
                                    request.segment() == 0
                                            ? data.getOrDefault(
                                                    request.tableName(), Collections.emptyList())
                                            : Collections.emptyList();
                            return ScanResponse.builder().items(items).build();
                        });
        return client;
    }

    private static Map<String, AttributeValue> item(Object... keyValues) {
        Map<String, AttributeValue> item = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            item.put((String) keyValues[i], (AttributeValue) keyValues[i + 1]);
        }
        return item;
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(String value) {
        return AttributeValue.builder().n(value).build();
    }

    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return deserialize(bytes.toByteArray());
    }

    @SuppressWarnings("unchecked")
    private static <T> T deserialize(byte[] bytes) throws Exception {
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) input.readObject();
        }
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<AmazonDynamoDBSourceSplit> {
        private final Map<Integer, List<AmazonDynamoDBSourceSplit>> assigned = new HashMap<>();
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};

        @Override
        public int currentParallelism() {
            return 2;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return new HashSet<>(Arrays.asList(0, 1));
        }

        @Override
        public void assignSplit(int subtaskId, List<AmazonDynamoDBSourceSplit> splits) {
            assigned.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }
}
