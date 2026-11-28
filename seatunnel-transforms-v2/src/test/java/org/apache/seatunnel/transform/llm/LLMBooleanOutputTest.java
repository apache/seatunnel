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

package org.apache.seatunnel.transform.llm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.nlpmodel.llm.LLMTransform;
import org.apache.seatunnel.transform.nlpmodel.llm.LLMTransformFactory;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.Model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Timeout(30)
class LLMBooleanOutputTest {

    private static final String INPUT_VALUE = "sensitive-row-value";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MockWebServer server;
    private LLMTransform transform;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            if (transform != null) {
                transform.close();
            }
        } finally {
            server.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("legacyBooleanOutputs")
    void preservesDefaultBooleanConversion(String response, boolean expected) throws IOException {
        configure("BOOLEAN", null);
        Assertions.assertEquals(expected, mapResponse(response).getField(1));
    }

    static Stream<Arguments> legacyBooleanOutputs() {
        return Stream.of(
                Arguments.of("[\"unknown\"]", false),
                Arguments.of("[\"\"]", false),
                Arguments.of("[null]", false),
                Arguments.of("[\" true \"]", false),
                Arguments.of("[\"yes\"]", false),
                Arguments.of("[1]", false),
                Arguments.of("[\"TRUE\"]", true),
                Arguments.of("[false]", false),
                Arguments.of("[false, true]", false));
    }

    @Test
    void explicitlyDisabledValidationPreservesCoercion() throws IOException {
        configure("BOOLEAN", false);
        Assertions.assertEquals(false, mapResponse("[\"unknown\"]").getField(1));
    }

    @ParameterizedTest
    @MethodSource("validBooleanOutputs")
    void acceptsBooleanLiteralsAndPreservesCaseCompatibility(String response, boolean expected)
            throws IOException {
        configure("BOOLEAN", true);
        Assertions.assertEquals(expected, mapResponse(response).getField(1));
    }

    static Stream<Arguments> validBooleanOutputs() {
        return Stream.of(
                Arguments.of("[true]", true),
                Arguments.of("[false]", false),
                Arguments.of("[True]", true),
                Arguments.of("[FALSE]", false),
                Arguments.of("[\"TrUe\"]", true),
                Arguments.of("[\"FaLsE\"]", false));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "[\"unknown-private-response\"]",
                "[\"\"]",
                "[null]",
                "[\" true \"]",
                "[\"false \"]",
                "[\"yes\"]",
                "[0]",
                "[]",
                "null",
                "[true, false]"
            })
    void strictValidationFailsWithoutExposingRowOrResponse(String response) throws IOException {
        configure("BOOLEAN", true);
        TransformException error =
                Assertions.assertThrows(TransformException.class, () -> mapResponse(response));
        Assertions.assertTrue(error.getMessage().contains("strict_boolean_output"));
        Assertions.assertTrue(error.getMessage().contains("true or false"));
        StringWriter stackTrace = new StringWriter();
        error.printStackTrace(new PrintWriter(stackTrace));
        Assertions.assertFalse(stackTrace.toString().contains(INPUT_VALUE));
        Assertions.assertFalse(stackTrace.toString().contains("unknown-private-response"));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(1, server.getRequestCount());
    }

    @ParameterizedTest
    @MethodSource("nonBooleanOutputs")
    void validationDoesNotChangeOtherOutputTypes(String type, String response, Object expected)
            throws IOException {
        configure(type, true);
        Assertions.assertEquals(expected, mapResponse(response).getField(1));
    }

    static Stream<Arguments> nonBooleanOutputs() {
        return Stream.of(
                Arguments.of("STRING", "[\"unknown\"]", "unknown"),
                Arguments.of("STRING", "[null]", "null"),
                Arguments.of("STRING", "[\"first\", \"second\"]", "first"),
                Arguments.of("INT", "[42]", 42),
                Arguments.of("BIGINT", "[2147483648]", 2147483648L),
                Arguments.of("DOUBLE", "[0.25]", 0.25D));
    }

    @Test
    void preservesNumericConversionFailure() throws IOException {
        configure("INT", true);
        RuntimeException error =
                Assertions.assertThrows(
                        RuntimeException.class, () -> mapResponse("[\"invalid-number\"]"));
        Assertions.assertTrue(error.getCause().getMessage().contains("RESPONSE_PARSE_ERROR"));
        assertSafe(error, "invalid-number");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"OPENAI", "MICROSOFT", "KIMIAI", "CUSTOM", "DEEPSEEK", "DOUBAO", "ZHIPU"})
    void inferenceFailureDoesNotExposeRowOrProviderBody(String provider) {
        TableTransformFactoryContext context =
                configuration("STRING", null, "CUSTOM".equals(provider), provider);
        transform = new LLMTransform(context.getOptions(), context.getCatalogTables().get(0));
        transform.getProducedCatalogTable();
        server.enqueue(
                new MockResponse().setResponseCode(503).setBody("private-provider-response"));
        RuntimeException error =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        StringWriter stack = new StringWriter();
        error.printStackTrace(new PrintWriter(stack));
        Assertions.assertFalse(stack.toString().contains(INPUT_VALUE), stack.toString());
        Assertions.assertFalse(
                stack.toString().contains("private-provider-response"), stack.toString());
        Assertions.assertTrue(stack.toString().contains("provider=" + provider), stack.toString());
        Assertions.assertTrue(stack.toString().contains("httpStatus=503"), stack.toString());
        Assertions.assertEquals(1, server.getRequestCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {"OPENAI", "MICROSOFT", "KIMIAI", "CUSTOM"})
    void malformedResponseDoesNotLeakParserContent(String provider) {
        configureProvider(provider, "STRING");
        server.enqueue(new MockResponse().setBody("{\"private-response-marker\": invalid-json}"));
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        assertSafe(failure, "private-response-marker");
        Assertions.assertTrue(failure.getCause().getMessage().contains("RESPONSE_PARSE_ERROR"));
        Assertions.assertEquals(1, server.getRequestCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {"INT", "BIGINT", "DOUBLE"})
    void numericConversionDoesNotLeakGeneratedContent(String type) throws IOException {
        configure(type, false);
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class, () -> mapResponse("[\"private-output-marker\"]"));
        assertSafe(failure, "private-output-marker");
        Assertions.assertTrue(failure.getCause().getMessage().contains("RESPONSE_PARSE_ERROR"));
    }

    @ParameterizedTest
    @ValueSource(ints = {302, 400, 401, 403, 429, 500, 503})
    void retainsStatusWithoutRetryingOrExposingErrorBody(int status) {
        configureProvider("CUSTOM", "STRING");
        // Do not allow HTTP redirection to turn this into another provider invocation.
        server.enqueue(new MockResponse().setResponseCode(status).setBody("private-error-marker"));
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        assertSafe(failure, "private-error-marker");
        Assertions.assertTrue(failure.getCause().getMessage().contains("httpStatus=" + status));
        Assertions.assertEquals(1, server.getRequestCount());
    }

    @Test
    void dropsUnsafeCauseAndSuppressedExceptionsFromInjectedModel() throws Exception {
        configure("STRING", false);
        transform.close();
        Field field = LLMTransform.class.getDeclaredField("model");
        field.setAccessible(true);
        field.set(
                transform,
                new Model() {
                    @Override
                    public java.util.List<String> inference(java.util.List<SeaTunnelRow> rows)
                            throws IOException {
                        IOException failure =
                                new IOException(
                                        "private-credential-marker", new IOException(INPUT_VALUE));
                        failure.addSuppressed(new IOException("private-prompt-marker"));
                        throw failure;
                    }

                    @Override
                    public void close() {}
                });
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        assertSafe(failure, "private-credential-marker", "private-prompt-marker");
        Assertions.assertNull(failure.getCause().getCause());
        Assertions.assertEquals(0, failure.getCause().getSuppressed().length);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CUSTOM", "OPENAI"})
    void subsequentSuccessfulInvocationIsUnchangedAfterFailure(String provider) throws IOException {
        configureProvider(provider, "STRING");
        server.enqueue(new MockResponse().setResponseCode(503).setBody("private-response-marker"));
        Assertions.assertThrows(
                RuntimeException.class,
                () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        Assertions.assertEquals("success", mapResponse("[\"success\"]").getField(1));
        Assertions.assertEquals(2, server.getRequestCount());
    }

    @Test
    void transportSetupDoesNotExposeCredentialBearingUri() {
        configureOverride("api_path", "http://private-credential-marker.invalid/invalid path");
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        assertSafe(failure, "private-credential-marker");
        Assertions.assertEquals(0, server.getRequestCount());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "private-model-marker\nforged-log-entry",
                "https://user:private-model-marker@host/model",
                "https://private-model-marker/model"
            })
    void omitsUnsafeModelMetadata(String modelName) {
        configureOverride("model", modelName);
        server.enqueue(new MockResponse().setResponseCode(401).setBody("private-response-marker"));
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        StringWriter stack = new StringWriter();
        failure.printStackTrace(new PrintWriter(stack));
        Assertions.assertFalse(stack.toString().contains("private-model-marker"), stack.toString());
        Assertions.assertTrue(stack.toString().contains("model=[omitted]"), stack.toString());
    }

    @Test
    void omitsModelMetadataContainingConfiguredCredential() {
        configureOverride("api_key", "test-model");
        server.enqueue(new MockResponse().setResponseCode(401));
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        Assertions.assertTrue(failure.getCause().getMessage().contains("model=[omitted]"));
        Assertions.assertFalse(failure.getCause().getMessage().contains("test-model"));
    }

    @Test
    void initializationDoesNotExposeInvalidProjection() {
        configureOverride(
                "inference_columns", Collections.singletonList("private-projection-marker"));
        RuntimeException failure =
                Assertions.assertThrows(RuntimeException.class, () -> transform.open());
        assertSafe(failure, "private-projection-marker");
        Assertions.assertTrue(failure.getCause().getMessage().contains("CONFIGURATION_ERROR"));
        Assertions.assertEquals(0, server.getRequestCount());
    }

    @Test
    void invalidProviderDoesNotEscapeDiagnosticSanitization() {
        configureOverride("model_provider", "private-provider-marker");
        RuntimeException failure =
                Assertions.assertThrows(RuntimeException.class, () -> transform.open());
        StringWriter stack = new StringWriter();
        failure.printStackTrace(new PrintWriter(stack));
        Assertions.assertFalse(
                stack.toString().contains("private-provider-marker"), stack.toString());
        Assertions.assertTrue(stack.toString().contains("CONFIGURATION_ERROR"), stack.toString());
    }

    @Test
    void customAuthorizationCredentialIsNotEchoedAsModelMetadata() {
        TableTransformFactoryContext context = configuration("STRING", false, true);
        Map<String, Object> options = new HashMap<>(context.getOptions().getSourceMap());
        options.put("model", "private-credential-marker");
        Map<String, Object> custom =
                new HashMap<>((Map<String, Object>) options.get("custom_config"));
        custom.put(
                "custom_request_headers",
                Collections.singletonMap("Authorization", "Bearer private-credential-marker"));
        options.put("custom_config", custom);
        transform =
                new LLMTransform(
                        ReadonlyConfig.fromMap(options), context.getCatalogTables().get(0));
        transform.getProducedCatalogTable();
        server.enqueue(new MockResponse().setResponseCode(401));
        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {INPUT_VALUE})));
        StringWriter stack = new StringWriter();
        failure.printStackTrace(new PrintWriter(stack));
        Assertions.assertFalse(
                stack.toString().contains("private-credential-marker"), stack.toString());
        Assertions.assertTrue(stack.toString().contains("model=[omitted]"), stack.toString());
    }

    private void configureOverride(String option, Object value) {
        TableTransformFactoryContext context = configuration("STRING", false, false);
        Map<String, Object> options = new HashMap<>(context.getOptions().getSourceMap());
        options.put(option, value);
        transform =
                new LLMTransform(
                        ReadonlyConfig.fromMap(options), context.getCatalogTables().get(0));
        transform.getProducedCatalogTable();
    }

    private void configureProvider(String provider, String type) {
        TableTransformFactoryContext context =
                configuration(type, null, "CUSTOM".equals(provider), provider);
        transform = new LLMTransform(context.getOptions(), context.getCatalogTables().get(0));
        transform.getProducedCatalogTable();
        transform.open();
    }

    private static void assertSafe(Throwable error, String... markers) {
        StringWriter stack = new StringWriter();
        error.printStackTrace(new PrintWriter(stack));
        Assertions.assertFalse(stack.toString().contains(INPUT_VALUE), stack.toString());
        for (String marker : markers) {
            Assertions.assertFalse(stack.toString().contains(marker), stack.toString());
        }
        Assertions.assertTrue(stack.toString().contains("provider="), stack.toString());
        Assertions.assertTrue(stack.toString().contains("model=test-model"), stack.toString());
    }

    @Test
    void preservesSchemaAndRowMetadata() throws IOException {
        configure("BOOLEAN", true);
        Assertions.assertEquals(
                BasicType.BOOLEAN_TYPE,
                transform
                        .getProducedCatalogTable()
                        .getTableSchema()
                        .getColumns()
                        .get(1)
                        .getDataType());
        SeaTunnelRow output = mapResponse("[true]");
        Assertions.assertEquals(INPUT_VALUE, output.getField(0));
        Assertions.assertEquals("test.input", output.getTableId());
        Assertions.assertEquals(RowKind.UPDATE_AFTER, output.getRowKind());
    }

    @Test
    void exposesOptionalValidationInFactoryRule() {
        Assertions.assertTrue(
                new LLMTransformFactory()
                        .optionRule().getOptionalOptions().stream()
                                .anyMatch(option -> "strict_boolean_output".equals(option.key())));
    }

    @Test
    void preservesLegacySerializationIdentifier() {
        Assertions.assertEquals(
                4711686225005641485L,
                ObjectStreamClass.lookup(LLMTransform.class).getSerialVersionUID());
    }

    @Test
    @SuppressWarnings("unchecked")
    void readsLegacyFactoryWrapperWithValidationDisabled() throws Exception {
        try (InputStream resource =
                getClass().getResourceAsStream("/llm/legacy-boolean-wrapper.base64")) {
            Assertions.assertNotNull(resource);
            try (ObjectInputStream input =
                    new ObjectInputStream(Base64.getMimeDecoder().wrap(resource)) {
                        {
                            enableResolveObject(true);
                        }

                        @Override
                        protected Object resolveObject(Object value) {
                            if ("http://127.0.0.1:1/legacy-fixture".equals(value)) {
                                // Only relocate the HTTP endpoint; leave serialized fields intact.
                                return server.url("/chat/completions").toString();
                            }
                            if (value instanceof LLMTransform) {
                                transform = (LLMTransform) value;
                            }
                            return value;
                        }
                    }) {
                SeaTunnelMapTransform<SeaTunnelRow> restored =
                        (SeaTunnelMapTransform<SeaTunnelRow>) input.readObject();
                Assertions.assertNotNull(transform);
                Assertions.assertEquals(false, mapResponse("[\"unknown\"]", restored).getField(1));
                Assertions.assertEquals(true, mapResponse("[\"TrUe\"]", restored).getField(1));
                SeaTunnelRow output = mapResponse("[\"FaLsE\"]", restored);
                Assertions.assertEquals(false, output.getField(1));
                Assertions.assertEquals(INPUT_VALUE, output.getField(0));
                Assertions.assertEquals("test.input", output.getTableId());
                Assertions.assertEquals(RowKind.UPDATE_AFTER, output.getRowKind());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void preservesValidationModeThroughFactoryWrapperSerialization(boolean strict)
            throws IOException, ClassNotFoundException {
        TableTransformFactoryContext context =
                configuration("BOOLEAN", strict ? true : null, false);
        byte[] serialized =
                SerializationUtils.serialize(
                        new LLMTransformFactory().createTransform(context).createTransform());
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serialized)) {
                    {
                        enableResolveObject(true);
                    }

                    @Override
                    protected Object resolveObject(Object value) {
                        if (value instanceof LLMTransform) {
                            // Retain the child for teardown; the factory wrapper has no close hook.
                            transform = (LLMTransform) value;
                        }
                        return value;
                    }
                }) {
            SeaTunnelMapTransform<SeaTunnelRow> restored =
                    (SeaTunnelMapTransform<SeaTunnelRow>) input.readObject();
            Assertions.assertNotNull(transform);
            if (strict) {
                Assertions.assertThrows(
                        TransformException.class, () -> mapResponse("[\"unknown\"]", restored));
            } else {
                Assertions.assertEquals(false, mapResponse("[\"unknown\"]", restored).getField(1));
            }
            Assertions.assertEquals(true, mapResponse("[\"TrUe\"]", restored).getField(1));
            Assertions.assertEquals(false, mapResponse("[\"FaLsE\"]", restored).getField(1));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"[\"unknown\"]", "unknown"})
    void validatesCustomArrayStringsAndPlainStrings(String content) throws IOException {
        configure("BOOLEAN", true, true);
        Assertions.assertThrows(TransformException.class, () -> mapResponse(content));
    }

    @Test
    void acceptsCustomPlainBooleanString() throws IOException {
        configure("BOOLEAN", true, true);
        Assertions.assertEquals(true, mapResponse("TRUE").getField(1));
    }

    private void configure(String type, Boolean strict) {
        configure(type, strict, false);
    }

    private void configure(String type, Boolean strict, boolean custom) {
        TableTransformFactoryContext context = configuration(type, strict, custom);
        transform = new LLMTransform(context.getOptions(), context.getCatalogTables().get(0));
        transform.getProducedCatalogTable();
        transform.open();
    }

    private TableTransformFactoryContext configuration(
            String type, Boolean strict, boolean custom) {
        return configuration(type, strict, custom, custom ? "CUSTOM" : "OPENAI");
    }

    private TableTransformFactoryContext configuration(
            String type, Boolean strict, boolean custom, String provider) {
        Map<String, Object> options = new HashMap<>();
        options.put("model_provider", provider);
        options.put("model", "test-model");
        options.put("api_key", "test-key");
        options.put("api_path", server.url("/chat/completions").toString());
        options.put("prompt", "Classify the input");
        options.put("output_data_type", type);
        if (strict != null) {
            options.put("strict_boolean_output", strict);
        }
        if (custom) {
            Map<String, Object> customOptions = new HashMap<>();
            customOptions.put(
                    "custom_request_headers",
                    Collections.singletonMap("Content-Type", "application/json"));
            customOptions.put("custom_request_body", Collections.singletonMap("input", "${input}"));
            customOptions.put("custom_response_parse", "$.choices[0].message.content");
            options.put("custom_config", customOptions);
        }
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        ConfigValidator.of(config).validate(new LLMTransformFactory().optionRule());
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.of("test", "input")),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "text",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test input");
        return new TableTransformFactoryContext(
                Collections.singletonList(table),
                config,
                Thread.currentThread().getContextClassLoader());
    }

    private SeaTunnelRow mapResponse(String content) throws IOException {
        return mapResponse(content, transform);
    }

    private SeaTunnelRow mapResponse(String content, SeaTunnelMapTransform<SeaTunnelRow> target)
            throws IOException {
        ObjectNode response = MAPPER.createObjectNode();
        response.putArray("choices").addObject().putObject("message").put("content", content);
        server.enqueue(new MockResponse().setBody(MAPPER.writeValueAsString(response)));
        SeaTunnelRow input = new SeaTunnelRow(new Object[] {INPUT_VALUE});
        input.setTableId("test.input");
        input.setRowKind(RowKind.UPDATE_AFTER);
        return target.map(input);
    }
}
