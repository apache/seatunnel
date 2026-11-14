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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Locale;

/**
 * Verifies explicit dirty-handling policy selection and isolation between plugin classloaders.
 * Lifecycle failures are checked through real closure output and preserved exception causality.
 */
class DirtyCollectorFactoryTest {

    private static final String SERVICE = "META-INF/services/" + Factory.class.getName();
    private final ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
    private final Locale previousLocale = Locale.getDefault();
    @TempDir Path temporaryDirectory;

    @AfterEach
    void restoreContext() {
        Thread.currentThread().setContextClassLoader(previousLoader);
        Locale.setDefault(previousLocale);
    }

    @Test
    void testAbsentAndExplicitNoOpConfigRemainDisabled() {
        Assertions.assertSame(
                NoOpDirtyRecordCollector.INSTANCE,
                DirtyRecordCollectorFactory.createCollector(null));
        Assertions.assertSame(
                NoOpDirtyRecordCollector.INSTANCE,
                DirtyCollectorConfigProcessor.processConfig(null, null));
        Assertions.assertSame(
                NoOpDirtyRecordCollector.INSTANCE,
                DirtyCollectorConfigProcessor.processConfig(
                        null, ConfigFactory.parseString("dirty.collector=null")));
        for (String type : Arrays.asList("noop", "NONE", " NoOp ")) {
            Assertions.assertSame(
                    NoOpDirtyRecordCollector.INSTANCE,
                    DirtyRecordCollectorFactory.createCollector(
                            ConfigFactory.empty()
                                    .withValue("type", ConfigValueFactory.fromAnyRef(type))));
        }
    }

    @Test
    void testMultiInputAllowsOnlyDisabledPoliciesBeforeInitialization() throws Exception {
        for (String disabled :
                Arrays.asList(
                        "",
                        "dirty.collector=null",
                        "dirty.collector.type=noop",
                        "dirty.collector.type=none")) {
            Assertions.assertSame(
                    NoOpDirtyRecordCollector.INSTANCE,
                    DirtyCollectorConfigProcessor.processConfig(
                            null, ConfigFactory.parseString(disabled), null, 2));
        }
        Config env = ConfigFactory.parseString("dirty.collector.type=log");
        Assertions.assertSame(
                NoOpDirtyRecordCollector.INSTANCE,
                DirtyCollectorConfigProcessor.processConfig(
                        env, ConfigFactory.parseString("dirty.collector=null"), null, 2));
        Path marker = temporaryDirectory.resolve("multi-input-init.txt");
        try (IsolatedLoader loader = loader("multi-input", FirstProvider.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            Config active =
                    ConfigFactory.parseString("dirty.collector.type=isolated")
                            .withValue(
                                    "dirty.collector.init_marker",
                                    ConfigValueFactory.fromAnyRef(marker.toString()));
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    DirtyCollectorConfigProcessor.processConfig(
                                            null, active, null, 2));
            Assertions.assertTrue(failure.getMessage().contains("single input catalog table"));
            Assertions.assertFalse(Files.exists(marker));
            DirtyRecordCollector single =
                    DirtyCollectorConfigProcessor.processConfig(null, active, null, 1);
            Assertions.assertEquals(Arrays.asList("collector-init"), Files.readAllLines(marker));
            single.collect(0, new SeaTunnelRow(new Object[] {-1}), null);
            Assertions.assertEquals(1, single.getDirtyRecordCount());
            single.close();
        }
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> DirtyCollectorConfigProcessor.processConfig(null, env, null, 2));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        DirtyCollectorConfigProcessor.processConfig(
                                null,
                                ConfigFactory.parseString(
                                        "dirty.collector.type=none\ndirty.validator.type=missing"),
                                null,
                                2));
    }

    @Test
    void testPlanningCleanupPreservesPrimaryAndClosesPreparedPolicy() throws Exception {
        Path marker = temporaryDirectory.resolve("planning-close.txt");
        RecordingCollector collector = new RecordingCollector();
        collector.init(
                ConfigFactory.parseString("fail_close=true")
                        .withValue("marker", ConfigValueFactory.fromAnyRef(marker.toString())));
        IllegalArgumentException primary = new IllegalArgumentException("sink-planning");
        DirtyCollectorConfigProcessor.closeAfterPlanningFailure(collector, primary);
        Assertions.assertEquals("sink-planning", primary.getMessage());
        Assertions.assertEquals("collector-close", primary.getSuppressed()[0].getMessage());
        Assertions.assertEquals(Arrays.asList("collector-close"), Files.readAllLines(marker));
        DirtyCollectorConfigProcessor.closeAfterPlanningFailure(null, primary);
        DirtyCollectorConfigProcessor.closeAfterPlanningFailure(
                NoOpDirtyRecordCollector.INSTANCE, null);
        Assertions.assertEquals(1, primary.getSuppressed().length);
    }

    @Test
    void testSkippedPlanningSurfacesCleanupFailure() throws Exception {
        Path marker = temporaryDirectory.resolve("skipped-close.txt");
        RecordingCollector collector = new RecordingCollector();
        collector.init(
                ConfigFactory.parseString("fail_close=true")
                        .withValue("marker", ConfigValueFactory.fromAnyRef(marker.toString())));
        IllegalStateException failure =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                DirtyCollectorConfigProcessor.closeAfterPlanningFailure(
                                        collector, null));
        Assertions.assertEquals("collector-close", failure.getCause().getMessage());
        Assertions.assertEquals(Arrays.asList("collector-close"), Files.readAllLines(marker));
    }

    @Test
    void testPresentCollectorRequiresNonBlankType() {
        for (String config : Arrays.asList("{}", "type=null", "type=\"   \"")) {
            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            DirtyRecordCollectorFactory.createCollector(
                                    ConfigFactory.parseString(config)));
        }
    }

    @Test
    void testSinkNullOverridesEnvPoliciesIndependently() throws Exception {
        Config env = ConfigFactory.parseString("dirty.collector.type=log");
        Assertions.assertSame(
                NoOpDirtyRecordCollector.INSTANCE,
                DirtyCollectorConfigProcessor.processConfig(
                        env, ConfigFactory.parseString("dirty.collector=null")));
        DirtyRecordCollector inherited = DirtyCollectorConfigProcessor.processConfig(env, null);
        Assertions.assertInstanceOf(LogDirtyRecordCollector.class, inherited);
        inherited.close();
        Config validatedEnv =
                env.withValue(
                        "dirty.validator.type",
                        ConfigValueFactory.fromAnyRef(RecordingValidator.class.getName()));
        DirtyRecordCollector withoutValidator =
                DirtyCollectorConfigProcessor.processConfig(
                        validatedEnv, ConfigFactory.parseString("dirty.validator=null"));
        Assertions.assertInstanceOf(LogDirtyRecordCollector.class, withoutValidator);
        withoutValidator.collect(0, new SeaTunnelRow(new Object[] {-1}), null, "rejected", null);
        Assertions.assertEquals(1, withoutValidator.getDirtyRecordCount());
        withoutValidator.close();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        DirtyCollectorConfigProcessor.processConfig(
                                null,
                                ConfigFactory.parseString(
                                        "dirty.collector.type=none\ndirty.validator.type=missing")));
    }

    @Test
    void testConfiguredValidatorRequiresNonBlankType() {
        for (String validator : Arrays.asList("{}", "{type=null}", "{type=\"   \"}")) {
            Config config =
                    ConfigFactory.parseString(
                            "dirty.collector.type=log\ndirty.validator=" + validator);
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> DirtyCollectorConfigProcessor.processConfig(null, config));
            Assertions.assertTrue(failure.getMessage().contains("dirty.validator"));
        }
    }

    @Test
    void testCustomCollectorsFollowEachInvocationLoaderAndRootLocale() throws Exception {
        Locale.setDefault(new Locale("tr", "TR"));
        try (IsolatedLoader first = loader("first", FirstProvider.class);
                IsolatedLoader second = loader("second", FirstProvider.class)) {
            Config config = ConfigFactory.parseString("type=isolated");
            Thread.currentThread().setContextClassLoader(first);
            DirtyRecordCollector one = DirtyRecordCollectorFactory.createCollector(config);
            Thread.currentThread().setContextClassLoader(second);
            DirtyRecordCollector two = DirtyRecordCollectorFactory.createCollector(config);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {-1});
            one.collect(0, row, null);
            two.collect(0, row, null);
            Assertions.assertEquals(1, one.getDirtyRecordCount());
            Assertions.assertEquals(1, two.getDirtyRecordCount());
            Assertions.assertSame(first, one.getClass().getClassLoader());
            Assertions.assertSame(second, two.getClass().getClassLoader());
            Assertions.assertNotSame(one.getClass(), two.getClass());
            one.close();
            two.close();
        }
    }

    @Test
    void testDuplicateCollectorProvidersAreRejected() throws Exception {
        try (IsolatedLoader loader =
                loader("duplicate", FirstProvider.class, SecondProvider.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    DirtyRecordCollectorFactory.createCollector(
                                            ConfigFactory.parseString("type=isolated")));
            Assertions.assertTrue(
                    failure.getMessage().contains("Multiple dirty.collector providers"));
        }
    }

    @Test
    void testDirectValidatorUsesActualContextLoader() throws Exception {
        try (IsolatedLoader loader = loader("direct")) {
            Thread.currentThread().setContextClassLoader(loader);
            DirtyDataValidator validator =
                    DirtyDataValidatorFactory.createValidator(
                            RecordingValidator.class.getName(), ConfigFactory.empty(), null);
            Assertions.assertSame(loader, validator.getClass().getClassLoader());
            Assertions.assertTrue(
                    validator.validate(new SeaTunnelRow(new Object[] {-1}), null).isDirty());
            Assertions.assertFalse(
                    validator.validate(new SeaTunnelRow(new Object[] {1}), null).isDirty());
            validator.close();
        }
    }

    @Test
    void testDuplicateValidatorsAreRejected() throws Exception {
        try (IsolatedLoader loader =
                loader(
                        "duplicate-validators",
                        RecordingValidator.class,
                        DuplicateValidator.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    DirtyDataValidatorFactory.createValidator(
                                            "isolated-validator", ConfigFactory.empty(), null));
            Assertions.assertTrue(
                    failure.getMessage().contains("Multiple dirty.validator providers"));
        }
    }

    @Test
    void testFoundWrongTypeNeverFallsBackToSpi() throws Exception {
        try (IsolatedLoader loader = loader("wrong-type", WrongTypeFallbackValidator.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    DirtyDataValidatorFactory.createValidator(
                                            "java.lang.String", ConfigFactory.empty(), null));
            Assertions.assertTrue(
                    failure.getMessage().contains("does not implement DirtyDataValidator"));
        }
    }

    @Test
    void testFoundConstructorAndInitializerFailuresRemainVisible() throws Exception {
        try (IsolatedLoader loader = loader("constructor")) {
            Thread.currentThread().setContextClassLoader(loader);
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    DirtyDataValidatorFactory.createValidator(
                                            ConstructorFailureValidator.class.getName(),
                                            ConfigFactory.empty(),
                                            null));
            Assertions.assertEquals("constructor-failed", failure.getCause().getMessage());
            ExceptionInInitializerError initialization =
                    Assertions.assertThrows(
                            ExceptionInInitializerError.class,
                            () ->
                                    DirtyDataValidatorFactory.createValidator(
                                            InitializationFailureValidator.class.getName(),
                                            ConfigFactory.empty(),
                                            null));
            Assertions.assertEquals(
                    "initialization-failed", initialization.getCause().getMessage());
        }
    }

    @Test
    void testCollectorInitFailureReleasesPartialCollector() throws Exception {
        Path marker = temporaryDirectory.resolve("collector-closed.txt");
        try (IsolatedLoader loader = loader("collector-init", FirstProvider.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            Config config =
                    ConfigFactory.parseString("type=isolated\nfail_init=true\nfail_close=true")
                            .withValue("marker", ConfigValueFactory.fromAnyRef(marker.toString()));
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> DirtyRecordCollectorFactory.createCollector(config));
            Assertions.assertEquals("collector-init", failure.getCause().getMessage());
            Assertions.assertEquals(
                    "collector-close", failure.getCause().getSuppressed()[0].getMessage());
            Assertions.assertEquals(Arrays.asList("collector-close"), Files.readAllLines(marker));
        }
    }

    @Test
    void testValidatorInitFailureReleasesBothResourcesWithoutMaskingPrimary() throws Exception {
        Path marker = temporaryDirectory.resolve("init-closed.txt");
        try (IsolatedLoader loader = loader("validator-init", FirstProvider.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            Config config =
                    ConfigFactory.parseString(
                                    "dirty.collector { type=isolated, fail_close=true }\n"
                                            + "dirty.validator { type=\""
                                            + RecordingValidator.class.getName()
                                            + "\", fail_init=true, fail_close=true }")
                            .withValue(
                                    "dirty.collector.marker",
                                    ConfigValueFactory.fromAnyRef(marker.toString()))
                            .withValue(
                                    "dirty.validator.marker",
                                    ConfigValueFactory.fromAnyRef(marker.toString()));
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> DirtyCollectorConfigProcessor.processConfig(null, config));
            Assertions.assertEquals("validator-init", failure.getCause().getMessage());
            Assertions.assertEquals(
                    "validator-close", failure.getCause().getSuppressed()[0].getMessage());
            Assertions.assertEquals("collector-close", failure.getSuppressed()[0].getMessage());
            Assertions.assertEquals(
                    Arrays.asList("validator-close", "collector-close"),
                    Files.readAllLines(marker));
        }
    }

    @Test
    void testValidationFailuresCannotPassRowsToDelegate() {
        RecordingCollector collector = new RecordingCollector();
        RuntimeException cause = new IllegalArgumentException("validation-failed");
        DirtyDataValidator throwing =
                (row, table) -> {
                    throw cause;
                };
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {-1});
        IllegalStateException failure =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new ValidatingDirtyRecordCollector(collector, throwing)
                                        .validateAndCollectIfDirty(0, row, null));
        Assertions.assertSame(cause, failure.getCause());
        DirtyDataValidator nullResult = (value, table) -> null;
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        new ValidatingDirtyRecordCollector(collector, nullResult)
                                .validateAndCollectIfDirty(0, row, null));
        Assertions.assertEquals(0, collector.getDirtyRecordCount());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ValidatingDirtyRecordCollector(
                                NoOpDirtyRecordCollector.INSTANCE, new RecordingValidator()));
        ValidatingDirtyRecordCollector valid =
                new ValidatingDirtyRecordCollector(collector, new RecordingValidator());
        Assertions.assertTrue(valid.validateAndCollectIfDirty(0, row, null));
        Assertions.assertSame(row, collector.lastRecord);
        Assertions.assertEquals(1, collector.getDirtyRecordCount());
    }

    @Test
    void testDualCloseFailurePreservesValidatorFailureAndClosesDelegate() throws Exception {
        Path marker = temporaryDirectory.resolve("dual-close.txt");
        Config config =
                ConfigFactory.parseString("fail_close=true")
                        .withValue("marker", ConfigValueFactory.fromAnyRef(marker.toString()));
        RecordingCollector collector = new RecordingCollector();
        collector.init(config);
        RecordingValidator validator = new RecordingValidator();
        validator.init(config, null);
        IOException failure =
                Assertions.assertThrows(
                        IOException.class,
                        () -> new ValidatingDirtyRecordCollector(collector, validator).close());
        Assertions.assertEquals("validator-close", failure.getMessage());
        Assertions.assertEquals("collector-close", failure.getSuppressed()[0].getMessage());
        Assertions.assertEquals(
                Arrays.asList("validator-close", "collector-close"), Files.readAllLines(marker));
    }

    private IsolatedLoader loader(String directory, Class<?>... providers) throws Exception {
        Path root = Files.createDirectories(temporaryDirectory.resolve(directory));
        Path descriptor = root.resolve(SERVICE);
        Files.createDirectories(descriptor.getParent());
        StringBuilder names = new StringBuilder();
        for (Class<?> provider : providers) {
            names.append(provider.getName()).append('\n');
        }
        Files.write(descriptor, names.toString().getBytes(StandardCharsets.UTF_8));
        return new IsolatedLoader(root.toUri().toURL());
    }

    /**
     * Loads genuine compiled fixture bytes child-first while sharing the production SPI classes.
     * Each loader exposes only its own descriptor, avoiding accidental parent SPI registrations.
     */
    private static class IsolatedLoader extends URLClassLoader {
        IsolatedLoader(URL directory) {
            super(new URL[] {directory}, DirtyCollectorFactoryTest.class.getClassLoader());
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            return SERVICE.equals(name) ? findResources(name) : super.getResources(name);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> type = findLoadedClass(name);
                if (type == null
                        && (name.equals(DirtyCollectorFactoryTest.class.getName())
                                || name.startsWith(
                                        DirtyCollectorFactoryTest.class.getName() + "$"))) {
                    String resource = name.replace('.', '/') + ".class";
                    try (InputStream input = getParent().getResourceAsStream(resource);
                            ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                        if (input == null) {
                            throw new ClassNotFoundException(name);
                        }
                        byte[] buffer = new byte[4096];
                        int count;
                        while ((count = input.read(buffer)) != -1) {
                            bytes.write(buffer, 0, count);
                        }
                        byte[] definition = bytes.toByteArray();
                        type = defineClass(name, definition, 0, definition.length);
                    } catch (IOException failure) {
                        throw new ClassNotFoundException(name, failure);
                    }
                }
                if (type == null) {
                    type = super.loadClass(name, false);
                }
                if (resolve) {
                    resolveClass(type);
                }
                return type;
            }
        }
    }

    /**
     * Provides a real collector whose class identity and collection result identify its loader. Its
     * uppercase identifier also detects locale-dependent lowercasing on Turkish systems.
     */
    public static class FirstProvider implements DirtyRecordCollectorProvider {
        @Override
        public String getType() {
            return "ISOLATED";
        }

        @Override
        public DirtyRecordCollector createCollector() {
            return new RecordingCollector();
        }
    }

    /**
     * Deliberately duplicates an existing provider identifier to exercise ambiguity rejection. It
     * remains a distinct provider class instead of relying on duplicate service-file lines.
     */
    public static class SecondProvider extends FirstProvider {}

    /**
     * Records actual collected rows and writes closure evidence to a configured file. Optional
     * failures expose cleanup ordering without relying on mock invocation counts.
     */
    public static class RecordingCollector implements DirtyRecordCollector {
        private long count;
        private String marker;
        private boolean failClose;
        private Object lastRecord;

        @Override
        public void init(Config config) throws IOException {
            if (config.hasPath("init_marker")) {
                Files.write(
                        Paths.get(config.getString("init_marker")),
                        Arrays.asList("collector-init"),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            }
            marker = config.hasPath("marker") ? config.getString("marker") : null;
            failClose = config.hasPath("fail_close") && config.getBoolean("fail_close");
            if (config.hasPath("fail_init") && config.getBoolean("fail_init")) {
                throw new IllegalStateException("collector-init");
            }
        }

        @Override
        public void collect(
                int task, Object row, Throwable failure, String reason, CatalogTable table) {
            count++;
            lastRecord = row;
        }

        @Override
        public long getDirtyRecordCount() {
            return count;
        }

        @Override
        public void close() throws IOException {
            if (marker != null) {
                Files.write(
                        Paths.get(marker),
                        Arrays.asList("collector-close"),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            }
            if (failClose) {
                throw new IOException("collector-close");
            }
        }
    }

    /**
     * Produces deterministic validation outcomes and resource-close evidence for factory tests.
     * Both direct loading and SPI discovery use this same real implementation contract.
     */
    public static class RecordingValidator implements DirtyDataValidator {
        private String marker;
        private boolean failClose;

        @Override
        public String factoryIdentifier() {
            return "isolated-validator";
        }

        @Override
        public void init(Config config, CatalogTable table) {
            marker = config.hasPath("marker") ? config.getString("marker") : null;
            failClose = config.hasPath("fail_close") && config.getBoolean("fail_close");
            if (config.hasPath("fail_init") && config.getBoolean("fail_init")) {
                throw new IllegalStateException("validator-init");
            }
        }

        @Override
        public ValidationResult validate(SeaTunnelRow row, CatalogTable table) {
            return ((Integer) row.getField(0)) < 0
                    ? ValidationResult.dirty("negative")
                    : ValidationResult.clean();
        }

        @Override
        public void close() throws IOException {
            if (marker != null) {
                Files.write(
                        Paths.get(marker),
                        Arrays.asList("validator-close"),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            }
            if (failClose) {
                throw new IOException("validator-close");
            }
        }
    }

    /**
     * Supplies a second validator with the same SPI identity so discovery must reject ambiguity.
     * Its class name is deliberately different from the first registered implementation.
     */
    public static class DuplicateValidator extends RecordingValidator {}

    /**
     * Would mask an explicitly found wrong class if direct lookup incorrectly fell back to SPI. The
     * real factory must reject String's type before this alternate provider is considered.
     */
    public static class WrongTypeFallbackValidator extends RecordingValidator {
        @Override
        public String factoryIdentifier() {
            return "java.lang.String";
        }
    }

    /**
     * Exposes constructor failures independently of initialization and provider discovery. A
     * present implementation must surface this error instead of selecting another policy.
     */
    public static class ConstructorFailureValidator extends RecordingValidator {
        public ConstructorFailureValidator() {
            throw new IllegalStateException("constructor-failed");
        }
    }

    /**
     * Fails during class initialization after the direct class has already been located. The
     * failure must remain visible and must not be interpreted as a missing SPI provider.
     */
    public static class InitializationFailureValidator extends RecordingValidator {
        private static final int VALUE = initialize();

        private static int initialize() {
            throw new IllegalStateException("initialization-failed");
        }
    }
}
