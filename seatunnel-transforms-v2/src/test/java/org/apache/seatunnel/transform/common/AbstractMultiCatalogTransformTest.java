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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AbstractMultiCatalogTransformTest {

    private static final Option<String> FIELD_NAME =
            Options.key("field_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Field name to append");

    private static final Option<String> REQUIRED_FIELD =
            Options.key("required_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Field that must exist before this rule is built");

    private static final Option<Boolean> FILTER_SCHEMA_CHANGE_EVENT =
            Options.key("filter_schema_change_event")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Return null for schema-change events");

    private static final Option<Boolean> FAIL_ON_SCHEMA_CHANGE_EVENT =
            Options.key("fail_on_schema_change_event")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Fail if a schema-change event reaches this rule");

    static Stream<Arguments> lifecycleSuccessCases() {
        return Stream.of(false, true)
                .flatMap(
                        flat ->
                                Stream.of(1, 3)
                                        .flatMap(
                                                tables ->
                                                        Stream.of(1, 2)
                                                                .map(
                                                                        rules ->
                                                                                Arguments.of(
                                                                                        flat,
                                                                                        tables,
                                                                                        rules))));
    }

    @ParameterizedTest
    @MethodSource("lifecycleSuccessCases")
    void multiTableLifecycleOpensAndClosesEachOwnedChildOnce(boolean flat, int tables, int rules) {
        LifecycleFixture fixture = new LifecycleFixture(flat, tables, rules);
        fixture.wrapper.open();
        fixture.wrapper.open();
        for (LifecycleChild child : fixture.children) {
            Assertions.assertEquals(1, child.opens);
            Assertions.assertEquals(1, child.resources);
        }
        for (CatalogTable table : fixture.tables) {
            SeaTunnelRow input = row(tablePath(table));
            SeaTunnelRow output =
                    flat
                            ? ((SeaTunnelFlatMapTransform<SeaTunnelRow>) fixture.wrapper)
                                    .flatMap(input)
                                    .get(0)
                            : ((SeaTunnelMapTransform<SeaTunnelRow>) fixture.wrapper).map(input);
            Assertions.assertSame(input, output);
        }
        fixture.wrapper.close();
        fixture.wrapper.close();
        for (LifecycleChild child : fixture.children) {
            Assertions.assertEquals(1, child.closes);
            Assertions.assertEquals(0, child.resources);
        }
    }

    static Stream<Arguments> lifecycleFailureCases() {
        return Stream.of(false, true)
                .flatMap(
                        flat ->
                                Stream.of(false, true)
                                        .flatMap(
                                                composed ->
                                                        Stream.of(0, 1, 2)
                                                                .map(
                                                                        position ->
                                                                                Arguments.of(
                                                                                        flat,
                                                                                        composed,
                                                                                        position))));
    }

    @ParameterizedTest
    @MethodSource("lifecycleFailureCases")
    void childOpenFailureStopsInitializationAndCleansOnce(
            boolean flat, boolean composed, int position) {
        LifecycleFixture fixture = new LifecycleFixture(flat, composed ? 1 : 3, composed ? 3 : 1);
        RuntimeException failure = new IllegalStateException("child-open-failed");
        fixture.children.get(position).openFailure = failure;
        RuntimeException cleanup = new IllegalStateException("cleanup-failed");
        fixture.children.get(0).closeFailure = cleanup;
        Assertions.assertSame(
                failure, Assertions.assertThrows(RuntimeException.class, fixture.wrapper::open));
        Assertions.assertSame(cleanup, failure.getSuppressed()[0]);
        Assertions.assertSame(
                failure, Assertions.assertThrows(RuntimeException.class, fixture.wrapper::open));
        Assertions.assertSame(
                cleanup, Assertions.assertThrows(RuntimeException.class, fixture.wrapper::close));
        for (int i = 0; i < fixture.children.size(); i++) {
            LifecycleChild child = fixture.children.get(i);
            Assertions.assertEquals(i <= position ? 1 : 0, child.opens);
            Assertions.assertEquals(1, child.closes);
            Assertions.assertEquals(0, child.resources);
        }
    }

    @ParameterizedTest
    @MethodSource("lifecycleSuccessCases")
    void closeContinuesAcrossFailuresAndRetainsFirst(boolean flat, int tables, int rules) {
        LifecycleFixture fixture = new LifecycleFixture(flat, tables, rules);
        fixture.wrapper.open();
        RuntimeException first = new IllegalStateException("first-close-failed");
        fixture.children.get(0).closeFailure = first;
        if (fixture.children.size() > 1) {
            fixture.children.get(1).closeFailure = new IllegalStateException("second-close-failed");
        }
        Assertions.assertSame(
                first, Assertions.assertThrows(RuntimeException.class, fixture.wrapper::close));
        if (fixture.children.size() > 1) {
            Assertions.assertEquals(1, first.getSuppressed().length);
        }
        Assertions.assertSame(
                first, Assertions.assertThrows(RuntimeException.class, fixture.wrapper::close));
        for (LifecycleChild child : fixture.children) {
            Assertions.assertEquals(1, child.closes);
            Assertions.assertEquals(0, child.resources);
        }
    }

    @Test
    void lifecycleRoundTripKeepsCallbacksAndIdentityChildren() throws Exception {
        for (boolean flat : new boolean[] {false, true}) {
            LifecycleFixture fixture = new LifecycleFixture(flat, 3, 2);
            CatalogTable last = fixture.tables.get(2);
            fixture.wrapper.transformMap.put(
                    tablePath(last),
                    flat ? new IdentityFlatMapTransform(last) : new IdentityMapTransform(last));
            fixture.children.subList(4, 6).clear();
            LifecycleFixture copy =
                    (LifecycleFixture)
                            SerializationUtils.deserialize(SerializationUtils.serialize(fixture));
            copy.wrapper.open();
            copy.wrapper.close();
            copy.wrapper.close();
            for (LifecycleChild child : copy.children) {
                Assertions.assertEquals(1, child.opens);
                Assertions.assertEquals(1, child.closes);
            }
            SeaTunnelRow input = row(tablePath(last));
            SeaTunnelRow output =
                    flat
                            ? ((SeaTunnelFlatMapTransform<SeaTunnelRow>) copy.wrapper)
                                    .flatMap(input)
                                    .get(0)
                            : ((SeaTunnelMapTransform<SeaTunnelRow>) copy.wrapper).map(input);
            Assertions.assertSame(input, output);
        }
    }

    @Test
    void deserializedWrapperHasFreshRuntimeOwnership() throws Exception {
        LifecycleFixture fixture = new LifecycleFixture(true, 1, 2);
        fixture.wrapper.open();
        fixture.wrapper.close();
        LifecycleFixture copy =
                (LifecycleFixture)
                        SerializationUtils.deserialize(SerializationUtils.serialize(fixture));
        copy.wrapper.open();
        copy.wrapper.close();
        for (LifecycleChild child : copy.children) {
            Assertions.assertEquals(2, child.opens);
            Assertions.assertEquals(2, child.closes);
            Assertions.assertEquals(0, child.resources);
        }
    }

    @Test
    void sharedChildIsOwnedByIdentityRatherThanNumberOfTableEntries() {
        LifecycleFixture fixture = new LifecycleFixture(false, 3, 1);
        LifecycleChild child = fixture.children.get(0);
        fixture.tables.forEach(table -> fixture.wrapper.transformMap.put(tablePath(table), child));
        fixture.wrapper.open();
        fixture.wrapper.close();
        Assertions.assertEquals(1, child.opens);
        Assertions.assertEquals(1, child.closes);
    }

    @Test
    void checkedCleanupExceptionDoesNotSkipSiblingsOrReplaceStartupFailure() {
        LifecycleFixture fixture = new LifecycleFixture(true, 1, 3);
        IOException original = new IOException("checked-open-failure");
        IOException cleanup = new IOException("checked-close-failure");
        fixture.children.get(1).openFailure = original;
        fixture.children.get(0).closeFailure = cleanup;
        Assertions.assertSame(
                original, Assertions.assertThrows(IOException.class, fixture.wrapper::open));
        Assertions.assertSame(cleanup, original.getSuppressed()[0]);
        Assertions.assertSame(
                cleanup, Assertions.assertThrows(IOException.class, fixture.wrapper::close));
        fixture.children.forEach(child -> Assertions.assertEquals(1, child.closes));
    }

    @Test
    void schemaConstructionFailureClosesPublishedAndUnpublishedChildren() {
        for (boolean flat : new boolean[] {false, true}) {
            List<LifecycleChild> created = new ArrayList<>();
            RuntimeException failure = new IllegalStateException("schema-initialization-failed");
            java.util.function.Function<CatalogTable, SeaTunnelTransform<SeaTunnelRow>> build =
                    table -> {
                        LifecycleChild child = new LifecycleChild(table);
                        child.resources = 1;
                        child.closeFailure = new IllegalStateException("cleanup-" + created.size());
                        created.add(child);
                        if (created.size() == 2) {
                            child.schemaFailure = failure;
                        }
                        return child;
                    };
            List<CatalogTable> tables =
                    Arrays.asList(
                            catalogTable(),
                            CatalogTableUtil.getCatalogTable(
                                    "catalog",
                                    "database",
                                    "schema",
                                    "second",
                                    catalogTable().getSeaTunnelRowType()));
            Assertions.assertSame(
                    failure,
                    Assertions.assertThrows(
                            RuntimeException.class,
                            () -> {
                                if (flat) {
                                    new TestFlatMapMultiCatalogTransform(tables, config()) {
                                        @Override
                                        protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
                                                CatalogTable table, ReadonlyConfig config) {
                                            return build.apply(table);
                                        }
                                    };
                                } else {
                                    new TestMapMultiCatalogTransform(tables, config()) {
                                        @Override
                                        protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
                                                CatalogTable table, ReadonlyConfig config) {
                                            return build.apply(table);
                                        }
                                    };
                                }
                            }));
            Assertions.assertEquals(2, created.size());
            Assertions.assertEquals(2, failure.getSuppressed().length);
            created.forEach(
                    child -> {
                        Assertions.assertEquals(1, child.closes);
                        Assertions.assertEquals(0, child.resources);
                    });
        }
    }

    @Test
    void closeBeforeOpenReleasesSchemaInitializedChildren() {
        LifecycleFixture fixture = new LifecycleFixture(false, 3, 1);
        fixture.children.forEach(child -> child.resources = 1);
        fixture.wrapper.close();
        fixture.wrapper.close();
        fixture.children.forEach(
                child -> {
                    Assertions.assertEquals(0, child.opens);
                    Assertions.assertEquals(1, child.closes);
                    Assertions.assertEquals(0, child.resources);
                });
    }

    @Test
    void repeatedCleanupExceptionDoesNotHideFirstFailure() {
        LifecycleFixture fixture = new LifecycleFixture(true, 1, 3);
        fixture.wrapper.open();
        RuntimeException same = new IllegalStateException("shared-close-failure");
        fixture.children.get(0).closeFailure = same;
        fixture.children.get(1).closeFailure = same;
        Assertions.assertSame(
                same, Assertions.assertThrows(RuntimeException.class, fixture.wrapper::close));
        fixture.children.forEach(child -> Assertions.assertEquals(1, child.closes));
    }

    @Test
    void concurrentCloseCallersAwaitTheSameCleanupFailure() throws Exception {
        LifecycleFixture fixture = new LifecycleFixture(true, 1, 3);
        fixture.wrapper.open();
        LifecycleChild child = fixture.children.get(0);
        child.closeEntered = new CountDownLatch(1);
        child.closeRelease = new CountDownLatch(1);
        RuntimeException failure = new IllegalStateException("shared-close-failure");
        child.closeFailure = failure;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> first =
                    executor.submit(
                            () ->
                                    Assertions.assertThrows(
                                            RuntimeException.class, fixture.wrapper::close));
            Assertions.assertTrue(child.closeEntered.await(5, TimeUnit.SECONDS));
            CountDownLatch secondStarted = new CountDownLatch(1);
            Future<Throwable> second =
                    executor.submit(
                            () -> {
                                secondStarted.countDown();
                                return Assertions.assertThrows(
                                        RuntimeException.class, fixture.wrapper::close);
                            });
            Assertions.assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(
                    TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
            child.closeRelease.countDown();
            Assertions.assertSame(failure, first.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(failure, second.get(5, TimeUnit.SECONDS));
            fixture.children.forEach(
                    c -> {
                        Assertions.assertEquals(1, c.closes);
                        Assertions.assertEquals(0, c.resources);
                    });
        } finally {
            child.closeRelease.countDown();
            executor.shutdownNow();
        }
    }

    private static final class LifecycleFixture implements Serializable {
        private final List<CatalogTable> tables = new ArrayList<>();
        private final List<LifecycleChild> children = new ArrayList<>();
        private final AbstractMultiCatalogTransform wrapper;

        private LifecycleFixture(boolean flat, int tableCount, int rules) {
            for (int i = 0; i < tableCount; i++) {
                tables.add(
                        CatalogTableUtil.getCatalogTable(
                                "catalog",
                                "database",
                                "schema",
                                "table" + i,
                                new SeaTunnelRowType(
                                        new String[] {"id"},
                                        new SeaTunnelDataType[] {BasicType.INT_TYPE})));
            }
            Map<String, Object>[] configs =
                    tables.stream()
                            .map(table -> tableRule(tablePath(table), "extra"))
                            .toArray(Map[]::new);
            wrapper =
                    flat
                            ? new TestFlatMapMultiCatalogTransform(tables, config(configs))
                            : new TestMapMultiCatalogTransform(tables, config(configs));
            for (CatalogTable table : tables) {
                List<SeaTunnelTransform<SeaTunnelRow>> chain = new ArrayList<>();
                for (int i = 0; i < rules; i++) {
                    LifecycleChild child = new LifecycleChild(table);
                    children.add(child);
                    chain.add(child);
                }
                wrapper.transformMap.put(tablePath(table), wrapper.composeTransforms(chain));
            }
        }
    }

    private static final class LifecycleChild
            implements SeaTunnelMapTransform<SeaTunnelRow>,
                    SeaTunnelFlatMapTransform<SeaTunnelRow> {
        private final CatalogTable table;
        private int opens;
        private int closes;
        private transient int resources;
        private Exception openFailure;
        private Exception closeFailure;
        private RuntimeException schemaFailure;
        private transient CountDownLatch closeEntered;
        private transient CountDownLatch closeRelease;

        private LifecycleChild(CatalogTable table) {
            this.table = table;
        }

        @Override
        public String getPluginName() {
            return "LifecycleChild";
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            if (schemaFailure != null) {
                throw schemaFailure;
            }
            return table;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(table);
        }

        @Override
        @lombok.SneakyThrows
        public void open() {
            opens++;
            resources = 1;
            if (openFailure != null) {
                throw openFailure;
            }
        }

        @Override
        @lombok.SneakyThrows
        public void close() {
            closes++;
            if (closeEntered != null) {
                closeEntered.countDown();
                if (!closeRelease.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out awaiting fixture close release");
                }
            }
            resources = 0;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            Assertions.assertEquals(1, opens);
            Assertions.assertEquals(0, closes);
            return row;
        }

        @Override
        public List<SeaTunnelRow> flatMap(SeaTunnelRow row) {
            return Collections.singletonList(map(row));
        }
    }

    @Test
    void defaultModeRejectsDuplicateExactTablePathRules() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(tableRule(tablePath, "first"), tableRule(tablePath, "second"));

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new TestMapMultiCatalogTransform(
                                        Collections.singletonList(table), config));

        Assertions.assertTrue(exception.getMessage().contains(tablePath));
    }

    @Test
    void defaultModeRejectsDuplicateExactTablePathRulesBeforeTableMatching() {
        CatalogTable table = catalogTable();
        String duplicateTablePath = "database.schema.customers";
        ReadonlyConfig config =
                config(
                        tableRule(duplicateTablePath, "first"),
                        tableRule(duplicateTablePath, "second"));

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new TestMapMultiCatalogTransform(
                                        Collections.singletonList(table), config));

        Assertions.assertTrue(exception.getMessage().contains(duplicateTablePath));
    }

    @Test
    void firstMatchModeUsesOnlyFirstExactTablePathRule() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(
                        TransformCommonOptions.RuleMatchMode.FIRST_MATCH,
                        tableRule(tablePath, "first"),
                        tableRule(tablePath, "second"));
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first"), fieldNames(transform));
        Assertions.assertEquals(2, output.getArity());
        Assertions.assertEquals("first", output.getField(1));
    }

    @Test
    void allMatchModeAppliesExactTablePathRulesInDeclarationOrder() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> secondRule = tableRule(tablePath, "second");
        secondRule.put(REQUIRED_FIELD.key(), "first");
        ReadonlyConfig config =
                config(
                        TransformCommonOptions.RuleMatchMode.ALL_MATCH,
                        tableRule(tablePath, "first"),
                        secondRule);
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first", "second"), fieldNames(transform));
        Assertions.assertEquals(3, output.getArity());
        Assertions.assertEquals("first", output.getField(1));
        Assertions.assertEquals("second", output.getField(2));
    }

    @Test
    void chainedMapTransformComputesLegacySerialVersionUid() {
        Assertions.assertEquals(
                8769738606872706499L,
                ObjectStreamClass.lookup(ChainedMapTransform.class).getSerialVersionUID());
    }

    @Test
    void chainedMapTransformReadsLegacyStreamAndMapsRows() throws Exception {
        ChainedMapTransform restored = readLegacyChainedMapTransform();
        String path = tablePath(catalogTable());

        SeaTunnelRow output = restored.map(row(path));

        Assertions.assertEquals(path, output.getTableId());
        Assertions.assertEquals(1, output.getField(0));
    }

    @Test
    void chainedMapTransformReadsLegacyStreamAndRefreshesInputTables() throws Exception {
        ChainedMapTransform restored = readLegacyChainedMapTransform();
        CatalogTable table = catalogTable();

        restored.setInputCatalogTables(Collections.singletonList(table));

        Assertions.assertEquals(tablePath(table), restored.map(row(tablePath(table))).getTableId());
    }

    @Test
    void chainedMapTransformReadsLegacyStreamAndMapsSchemaChanges() throws Exception {
        ChainedMapTransform restored = readLegacyChainedMapTransform();
        CatalogTable table = catalogTable();

        restored.mapSchemaChangeEvent(addColumnEvent(table));

        Assertions.assertEquals(tablePath(table), restored.map(row(tablePath(table))).getTableId());
    }

    @Test
    void chainedMapTransformPreservesExtraTableRoutingAfterCurrentRoundTrip() throws Exception {
        CatalogTable table = catalogTable();
        CatalogTable extraTable =
                CatalogTableUtil.getCatalogTable(
                        "catalog", "database", "schema", "errors", table.getSeaTunnelRowType());
        ChainedMapTransform transform =
                new ChainedMapTransform(
                        Arrays.asList(
                                new ExtraTableRoutingTransform(table, extraTable),
                                new FailOnMapTransform(table)));
        byte[] bytes = SerializationUtils.serialize(transform);

        ChainedMapTransform restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            restored = (ChainedMapTransform) input.readObject();
        }

        restored.setInputCatalogTables(Collections.singletonList(table));

        Assertions.assertEquals(
                tablePath(extraTable), restored.map(row(tablePath(table))).getTableId());
    }

    @Test
    void allMatchModeWorksForFlatMapTransforms() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        ReadonlyConfig config =
                config(
                        TransformCommonOptions.RuleMatchMode.ALL_MATCH,
                        tableRule(tablePath, "first"),
                        tableRule(tablePath, "second"));
        TestFlatMapMultiCatalogTransform transform =
                new TestFlatMapMultiCatalogTransform(Collections.singletonList(table), config);

        List<SeaTunnelRow> outputRows = transform.flatMap(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "first", "second"), fieldNames(transform));
        Assertions.assertEquals(1, outputRows.size());
        Assertions.assertEquals("first", outputRows.get(0).getField(1));
        Assertions.assertEquals("second", outputRows.get(0).getField(2));
    }

    @Test
    void chainedMapTransformStopsSchemaChangeAfterFilteredEvent() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> firstRule = tableRule(tablePath, "first");
        firstRule.put(FILTER_SCHEMA_CHANGE_EVENT.key(), true);
        Map<String, Object> secondRule = tableRule(tablePath, "second");
        secondRule.put(FAIL_ON_SCHEMA_CHANGE_EVENT.key(), true);
        ReadonlyConfig config =
                config(TransformCommonOptions.RuleMatchMode.ALL_MATCH, firstRule, secondRule);
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(Collections.singletonList(table), config);

        SchemaChangeEvent event = transform.mapSchemaChangeEvent(addColumnEvent(table));

        Assertions.assertNull(event);
    }

    @Test
    void chainedMapTransformClosesEveryTransformAfterFailure() {
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        ChainedMapTransform transform =
                new ChainedMapTransform(
                        Arrays.asList(
                                new CloseTrackingMapTransform(firstClosed, true),
                                new CloseTrackingMapTransform(secondClosed, false)));

        RuntimeException closeFailure =
                Assertions.assertThrows(RuntimeException.class, transform::close);

        Assertions.assertEquals("expected close failure", closeFailure.getMessage());
        Assertions.assertTrue(firstClosed.get());
        Assertions.assertTrue(secondClosed.get());
    }

    @Test
    void chainedFlatMapTransformStopsSchemaChangeAfterFilteredEvent() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> firstRule = tableRule(tablePath, "first");
        firstRule.put(FILTER_SCHEMA_CHANGE_EVENT.key(), true);
        Map<String, Object> secondRule = tableRule(tablePath, "second");
        secondRule.put(FAIL_ON_SCHEMA_CHANGE_EVENT.key(), true);
        ReadonlyConfig config =
                config(TransformCommonOptions.RuleMatchMode.ALL_MATCH, firstRule, secondRule);
        TestFlatMapMultiCatalogTransform transform =
                new TestFlatMapMultiCatalogTransform(Collections.singletonList(table), config);

        SchemaChangeEvent event = transform.mapSchemaChangeEvent(addColumnEvent(table));

        Assertions.assertNull(event);
    }

    @Test
    void tableMatchRegexIsFallbackWhenExactTablePathRuleDoesNotMatch() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TransformCommonOptions.TABLE_MATCH_REGEX.key(), ".*orders");
        configMap.put(FIELD_NAME.key(), "fallback");
        configMap.put(
                TransformCommonOptions.MULTI_TABLES.key(),
                Collections.singletonList(tableRule("database.schema.customers", "specific")));
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(
                        Collections.singletonList(table), ReadonlyConfig.fromMap(configMap));

        SeaTunnelRow output = transform.map(row(tablePath));

        Assertions.assertEquals(Arrays.asList("id", "fallback"), fieldNames(transform));
        Assertions.assertEquals("fallback", output.getField(1));
    }

    @Test
    void identityTransformIsUsedWhenNoRuleMatches() {
        CatalogTable table = catalogTable();
        String tablePath = tablePath(table);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TransformCommonOptions.TABLE_MATCH_REGEX.key(), "does-not-match");
        TestMapMultiCatalogTransform transform =
                new TestMapMultiCatalogTransform(
                        Collections.singletonList(table), ReadonlyConfig.fromMap(configMap));
        SeaTunnelRow input = row(tablePath);

        SeaTunnelRow output = transform.map(input);

        Assertions.assertSame(input, output);
        Assertions.assertEquals(Collections.singletonList("id"), fieldNames(transform));
    }

    private static ReadonlyConfig config(Map<String, Object>... tableRules) {
        return config(null, tableRules);
    }

    private static ReadonlyConfig config(
            TransformCommonOptions.RuleMatchMode ruleMatchMode, Map<String, Object>... tableRules) {
        Map<String, Object> configMap = new HashMap<>();
        if (ruleMatchMode != null) {
            configMap.put(TransformCommonOptions.RULE_MATCH_MODE.key(), ruleMatchMode);
        }
        configMap.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(tableRules));
        return ReadonlyConfig.fromMap(configMap);
    }

    private static Map<String, Object> tableRule(String tablePath, String fieldName) {
        Map<String, Object> rule = new HashMap<>();
        rule.put(TransformCommonOptions.TABLE_PATH.key(), tablePath);
        rule.put(FIELD_NAME.key(), fieldName);
        return rule;
    }

    private static CatalogTable catalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return CatalogTableUtil.getCatalogTable("catalog", "database", "schema", "orders", rowType);
    }

    // Fixture serialized from commit 02b156e45, before extraTablePaths was added. It verifies
    // ChainedMapTransform compatibility only; its child transforms use current support classes.
    private static ChainedMapTransform readLegacyChainedMapTransform()
            throws IOException, ClassNotFoundException {
        try (InputStream resource =
                AbstractMultiCatalogTransformTest.class.getResourceAsStream(
                        "/serialization/legacy-chained-map.base64")) {
            Assertions.assertNotNull(resource);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(resource, StandardCharsets.US_ASCII));
            String line;
            while ((line = reader.readLine()) != null && !"-----BEGIN BASE64-----".equals(line)) {
                // Skip the license header.
            }
            Assertions.assertNotNull(line);
            StringBuilder encoded = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                encoded.append(line);
            }
            try (ObjectInputStream input =
                    new ObjectInputStream(
                            new ByteArrayInputStream(
                                    Base64.getDecoder().decode(encoded.toString())))) {
                return (ChainedMapTransform) input.readObject();
            }
        }
    }

    private static String tablePath(CatalogTable table) {
        return table.getTableId().toTablePath().toString();
    }

    private static SeaTunnelRow row(String tablePath) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(tablePath);
        return row;
    }

    private static SchemaChangeEvent addColumnEvent(CatalogTable table) {
        return AlterTableAddColumnEvent.add(
                table.getTableId(),
                PhysicalColumn.of("created_at", BasicType.STRING_TYPE, 0L, true, null, null));
    }

    private static List<String> fieldNames(SeaTunnelTransform<SeaTunnelRow> transform) {
        return transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    private static SeaTunnelRow appendField(SeaTunnelRow inputRow, String fieldName) {
        Object[] fields = Arrays.copyOf(inputRow.getFields(), inputRow.getArity() + 1);
        fields[fields.length - 1] = fieldName;
        SeaTunnelRow outputRow = new SeaTunnelRow(fields);
        outputRow.setTableId(inputRow.getTableId());
        outputRow.setRowKind(inputRow.getRowKind());
        if (inputRow.getOptionsOrNull() != null) {
            outputRow.setOptions(inputRow.getOptionsOrNull());
        }
        return outputRow;
    }

    private static TableSchema appendField(CatalogTable inputCatalogTable, String fieldName) {
        List<Column> columns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toCollection(ArrayList::new));
        columns.add(PhysicalColumn.of(fieldName, BasicType.STRING_TYPE, 0L, true, null, null));
        return TableSchema.builder()
                .columns(columns)
                .primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey())
                .constraintKey(inputCatalogTable.getTableSchema().getConstraintKeys())
                .build();
    }

    private static void assertRequiredField(CatalogTable inputCatalogTable, ReadonlyConfig config) {
        config.getOptional(REQUIRED_FIELD)
                .ifPresent(
                        field ->
                                inputCatalogTable
                                        .getTableSchema()
                                        .toPhysicalRowDataType()
                                        .indexOf(field));
    }

    private static class TestMapMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

        private TestMapMultiCatalogTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        @Override
        public String getPluginName() {
            return "TestMap";
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
                CatalogTable inputCatalogTable, ReadonlyConfig config) {
            return new AppendFieldMapTransform(inputCatalogTable, config);
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(
                CatalogTable catalogTable) {
            return new IdentityMapTransform(catalogTable);
        }
    }

    private static class CloseTrackingMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

        private final AtomicBoolean closed;
        private final boolean failOnClose;

        private CloseTrackingMapTransform(AtomicBoolean closed, boolean failOnClose) {
            this.closed = closed;
            this.failOnClose = failOnClose;
        }

        @Override
        public String getPluginName() {
            return "CloseTracking";
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return row;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return catalogTable();
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(getProducedCatalogTable());
        }

        @Override
        public void close() {
            closed.set(true);
            if (failOnClose) {
                throw new RuntimeException("expected close failure");
            }
        }
    }

    private static class ExtraTableRoutingTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

        private final CatalogTable table;
        private final CatalogTable extraTable;

        private ExtraTableRoutingTransform(CatalogTable table, CatalogTable extraTable) {
            this.table = table;
            this.extraTable = extraTable;
        }

        @Override
        public String getPluginName() {
            return "ExtraTableRouting";
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            row.setTableId(tablePath(extraTable));
            return row;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return table;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Arrays.asList(table, extraTable);
        }
    }

    private static class FailOnMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

        private final CatalogTable table;

        private FailOnMapTransform(CatalogTable table) {
            this.table = table;
        }

        @Override
        public String getPluginName() {
            return "FailOnMap";
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            throw new AssertionError("Extra-table row must skip the remaining rules");
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return table;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(table);
        }
    }

    private static class TestFlatMapMultiCatalogTransform
            extends AbstractMultiCatalogFlatMapTransform {

        private TestFlatMapMultiCatalogTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        @Override
        public String getPluginName() {
            return "TestFlatMap";
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
                CatalogTable inputCatalogTable, ReadonlyConfig config) {
            return new AppendFieldFlatMapTransform(inputCatalogTable, config);
        }

        @Override
        protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(
                CatalogTable catalogTable) {
            return new IdentityFlatMapTransform(catalogTable);
        }
    }

    private static class AppendFieldMapTransform extends AbstractCatalogSupportMapTransform {

        private final String fieldName;

        private final boolean filterSchemaChangeEvent;

        private final boolean failOnSchemaChangeEvent;

        private AppendFieldMapTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
            super(inputCatalogTable);
            assertRequiredField(inputCatalogTable, config);
            this.fieldName = config.get(FIELD_NAME);
            this.filterSchemaChangeEvent = config.get(FILTER_SCHEMA_CHANGE_EVENT);
            this.failOnSchemaChangeEvent = config.get(FAIL_ON_SCHEMA_CHANGE_EVENT);
        }

        @Override
        public String getPluginName() {
            return "AppendFieldMap";
        }

        @Override
        protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
            return appendField(inputRow, fieldName);
        }

        @Override
        public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
            if (failOnSchemaChangeEvent) {
                throw new AssertionError("Schema-change event should not reach this rule");
            }
            if (filterSchemaChangeEvent) {
                return null;
            }
            return super.mapSchemaChangeEvent(event);
        }

        @Override
        protected TableSchema transformTableSchema() {
            return appendField(inputCatalogTable, fieldName);
        }

        @Override
        protected TableIdentifier transformTableIdentifier() {
            return inputCatalogTable.getTableId().copy();
        }
    }

    private static class AppendFieldFlatMapTransform
            extends AbstractCatalogSupportFlatMapTransform {

        private final String fieldName;

        private final boolean filterSchemaChangeEvent;

        private final boolean failOnSchemaChangeEvent;

        private AppendFieldFlatMapTransform(CatalogTable inputCatalogTable, ReadonlyConfig config) {
            super(inputCatalogTable);
            assertRequiredField(inputCatalogTable, config);
            this.fieldName = config.get(FIELD_NAME);
            this.filterSchemaChangeEvent = config.get(FILTER_SCHEMA_CHANGE_EVENT);
            this.failOnSchemaChangeEvent = config.get(FAIL_ON_SCHEMA_CHANGE_EVENT);
        }

        @Override
        public String getPluginName() {
            return "AppendFieldFlatMap";
        }

        @Override
        protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
            return Collections.singletonList(appendField(inputRow, fieldName));
        }

        @Override
        public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
            if (failOnSchemaChangeEvent) {
                throw new AssertionError("Schema-change event should not reach this rule");
            }
            if (filterSchemaChangeEvent) {
                return null;
            }
            return event;
        }

        @Override
        protected TableSchema transformTableSchema() {
            return appendField(inputCatalogTable, fieldName);
        }

        @Override
        protected TableIdentifier transformTableIdentifier() {
            return inputCatalogTable.getTableId().copy();
        }
    }
}
