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

package org.apache.seatunnel.e2e.connector.elasticsearch;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.auth.AuthenticationProvider;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.auth.AuthenticationProviderFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class ElasticsearchAuthIT extends TestSuiteBase implements TestResource {

    private static final String ELASTICSEARCH_IMAGE = "elasticsearch:8.9.0";
    private static final long INDEX_REFRESH_DELAY = 2000L;

    // Test data constants
    private static final String TEST_INDEX = "auth_test_index";
    private static final String VALID_USERNAME = "elastic";
    private static final String VALID_PASSWORD = "elasticsearch";
    private static final String INVALID_USERNAME = "wrong_user";
    private static final String INVALID_PASSWORD = "wrong_password";

    // API Key test constants - will be set dynamically after container starts
    private String validApiKeyId;
    private String validApiKeySecret;
    private String validEncodedApiKey;
    private static final String INVALID_API_KEY_ID = "invalid-key-id";
    private static final String INVALID_API_KEY_SECRET = "invalid-key-secret";

    private ElasticsearchContainer elasticsearchContainer;
    private EsRestClient esRestClient;
    private ObjectMapper objectMapper = new ObjectMapper();
    private CloseableHttpClient httpClient;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        // Initialize HTTP client with SSL trust all strategy
        initializeHttpClient();

        // Start Elasticsearch container
        elasticsearchContainer =
                new ElasticsearchContainer(
                                DockerImageName.parse(ELASTICSEARCH_IMAGE)
                                        .asCompatibleSubstituteFor(
                                                "docker.elastic.co/elasticsearch/elasticsearch"))
                        .withNetwork(NETWORK)
                        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                        .withEnv("xpack.security.authc.api_key.enabled", "true")
                        .withNetworkAliases("elasticsearch")
                        .withPassword("elasticsearch")
                        .withStartupAttempts(5)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("elasticsearch:8.9.0")));
        Startables.deepStart(Stream.of(elasticsearchContainer)).join();
        log.info("Elasticsearch container started");

        // Wait for Elasticsearch to be ready and create real API keys
        waitForElasticsearchReady();
        createRealApiKeys();

        // Initialize ES client for test data setup
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        configMap.put("username", "elastic");
        configMap.put("password", "elasticsearch");
        configMap.put("tls_verify_certificate", false);
        configMap.put("tls_verify_hostname", false);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        esRestClient = EsRestClient.createInstance(config);
        createTestIndex();
        insertTestData();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (esRestClient != null) {
            esRestClient.close();
        }
        if (httpClient != null) {
            httpClient.close();
        }
        if (elasticsearchContainer != null) {
            elasticsearchContainer.stop();
        }
    }

    /** Initialize HTTP client with SSL trust all strategy for testing */
    private void initializeHttpClient()
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        httpClient =
                HttpClients.custom()
                        .setSSLContext(
                                SSLContextBuilder.create()
                                        .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                                        .build())
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .build();
        log.info("HTTP client initialized with SSL trust all strategy");
    }

    /** Wait for Elasticsearch to be ready */
    private void waitForElasticsearchReady() throws IOException, InterruptedException {
        String elasticsearchUrl = "https://" + elasticsearchContainer.getHttpHostAddress();
        String healthUrl = elasticsearchUrl + "/_cluster/health";

        log.info("Waiting for Elasticsearch to be ready at: {}", healthUrl);

        for (int i = 0; i < 30; i++) {
            try {
                HttpGet request = new HttpGet(healthUrl);
                String auth =
                        Base64.getEncoder()
                                .encodeToString(
                                        (VALID_USERNAME + ":" + VALID_PASSWORD)
                                                .getBytes(StandardCharsets.UTF_8));
                request.setHeader("Authorization", "Basic " + auth);

                HttpResponse response = httpClient.execute(request);
                if (response.getStatusLine().getStatusCode() == 200) {
                    log.info("Elasticsearch is ready");
                    return;
                }
            } catch (Exception e) {
                log.debug("Elasticsearch not ready yet, attempt {}/30: {}", i + 1, e.getMessage());
            }

            TimeUnit.SECONDS.sleep(2);
        }

        throw new RuntimeException("Elasticsearch failed to become ready within timeout");
    }

    /** Create real API keys using Elasticsearch API */
    private void createRealApiKeys() throws IOException {
        String elasticsearchUrl = "https://" + elasticsearchContainer.getHttpHostAddress();
        String apiKeyUrl = elasticsearchUrl + "/_security/api_key";

        log.info("Creating real API key at: {}", apiKeyUrl);

        String requestBody =
                "{\n"
                        + "  \"name\": \"seatunnel-test-api-key\",\n"
                        + "  \"role_descriptors\": {\n"
                        + "    \"seatunnel_test_role\": {\n"
                        + "      \"cluster\": [\"manage\"],\n"
                        + "      \"indices\": [\n"
                        + "        {\n"
                        + "          \"names\": [\""
                        + TEST_INDEX
                        + "\", \"auth_test_*\", \"test_*\", \"*_target\"],\n"
                        + "          \"privileges\": [\"all\"]\n"
                        + "        }\n"
                        + "      ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"metadata\": {\n"
                        + "    \"application\": \"seatunnel-test\",\n"
                        + "    \"environment\": \"integration-test\"\n"
                        + "  }\n"
                        + "}";

        HttpPost request = new HttpPost(apiKeyUrl);
        String auth =
                Base64.getEncoder()
                        .encodeToString(
                                (VALID_USERNAME + ":" + VALID_PASSWORD)
                                        .getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + auth);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));

        HttpResponse response = httpClient.execute(request);
        String responseBody = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Failed to create API key: " + responseBody);
        }

        // Parse response to extract API key details
        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            validApiKeyId = jsonResponse.get("id").asText();
            validApiKeySecret = jsonResponse.get("api_key").asText();
            validEncodedApiKey =
                    Base64.getEncoder()
                            .encodeToString(
                                    (validApiKeyId + ":" + validApiKeySecret)
                                            .getBytes(StandardCharsets.UTF_8));

            log.info(
                    "API Key created successfully - ID: {}, Secret: {}, Encoded: {}",
                    validApiKeyId,
                    validApiKeySecret,
                    validEncodedApiKey);

            // Verify the API key works
            verifyApiKey();

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse API key response: " + responseBody, e);
        }
    }

    /** Verify that the created API key works */
    private void verifyApiKey() throws IOException {
        String elasticsearchUrl = "https://" + elasticsearchContainer.getHttpHostAddress();
        String authUrl = elasticsearchUrl + "/_security/_authenticate";

        HttpGet request = new HttpGet(authUrl);
        request.setHeader("Authorization", "ApiKey " + validEncodedApiKey);

        HttpResponse response = httpClient.execute(request);
        String responseBody = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() == 200) {
            log.info("API Key verification successful: {}", responseBody);
        } else {
            throw new RuntimeException("API Key verification failed: " + responseBody);
        }
    }

    private void createTestIndex() throws Exception {
        String mapping =
                "{"
                        + "\"mappings\": {"
                        + "\"properties\": {"
                        + "\"id\": {\"type\": \"integer\"},"
                        + "\"name\": {\"type\": \"text\"},"
                        + "\"value\": {\"type\": \"double\"}"
                        + "}"
                        + "}"
                        + "}";

        log.info("Creating test index: {}", TEST_INDEX);

        try {
            esRestClient.createIndex(TEST_INDEX, mapping);
            log.info("Test index '{}' created successfully", TEST_INDEX);
        } catch (Exception e) {
            log.error("Failed to create test index: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create test index: " + TEST_INDEX, e);
        }
    }

    private void insertTestData() throws Exception {
        StringBuilder requestBody = new StringBuilder();
        String indexHeader = "{\"index\":{\"_index\":\"" + TEST_INDEX + "\"}}\n";

        for (int i = 1; i <= 3; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("id", i);
            doc.put("name", "test_" + i);
            doc.put("value", i * 10.5);

            requestBody.append(indexHeader);
            requestBody.append(objectMapper.writeValueAsString(doc));
            requestBody.append("\n");
        }

        log.info("Inserting test data into index: {}", TEST_INDEX);

        try {
            BulkResponse response = esRestClient.bulk(requestBody.toString());
            if (response.isErrors()) {
                log.error("Bulk insert had errors: {}", response.getResponse());
                throw new RuntimeException("Failed to insert test data: " + response.getResponse());
            }

            Thread.sleep(INDEX_REFRESH_DELAY);
            log.info("Test data inserted successfully - {} documents", 3);
        } catch (Exception e) {
            log.error("Failed to insert test data", e);
            throw new RuntimeException("Failed to insert test data", e);
        }
    }

    // Helper methods for creating configurations
    private Map<String, Object> createBasicAuthConfig(String username, String password) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        configMap.put("username", username);
        configMap.put("password", password);
        configMap.put("tls_verify_certificate", false);
        configMap.put("tls_verify_hostname", false);

        return configMap;
    }

    private Map<String, Object> createApiKeyConfig(String keyId, String keySecret) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        config.put("auth_type", "api_key");
        config.put("auth.api_key_id", keyId);
        config.put("auth.api_key", keySecret);
        config.put("tls_verify_certificate", false);
        config.put("tls_verify_hostname", false);
        return config;
    }

    private Map<String, Object> createApiKeyEncodedConfig(String encodedKey) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        config.put("auth_type", "api_key_encoded");
        config.put("auth.api_key_encoded", encodedKey);
        config.put("tls_verify_certificate", false);
        config.put("tls_verify_hostname", false);
        return config;
    }

    // ==================== Basic Authentication Tests ====================

    /** Test successful basic authentication with valid credentials */
    @Test
    public void testBasicAuthenticationSuccess() throws Exception {
        log.info("=== Testing Basic Authentication Success ===");

        Map<String, Object> config = createBasicAuthConfig(VALID_USERNAME, VALID_PASSWORD);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "basic", provider.getAuthType(), "Provider should be basic auth type");

        // Test client creation and functionality
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");

            // Verify client can perform operations
            long docCount = client.getIndexDocsCount(TEST_INDEX).get(0).getDocsCount();
            Assertions.assertTrue(
                    docCount > 0, "Should be able to query index with valid credentials");

            log.info("✓ Basic authentication success test passed - {} documents found", docCount);
        }
    }

    /** Test basic authentication failure with invalid credentials */
    @Test
    public void testBasicAuthenticationFailure() throws Exception {
        log.info("=== Testing Basic Authentication Failure ===");

        Map<String, Object> config = createBasicAuthConfig(INVALID_USERNAME, INVALID_PASSWORD);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation (should succeed)
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(
                provider,
                "Authentication provider should be created even with invalid credentials");
        Assertions.assertEquals(
                "basic", provider.getAuthType(), "Provider should be basic auth type");

        // Test client creation (should succeed)
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created");

            // Test operation (should fail with authentication error)
            Exception exception =
                    Assertions.assertThrows(
                            Exception.class,
                            () -> {
                                client.getIndexDocsCount(TEST_INDEX);
                            },
                            "Should throw exception when using invalid credentials");

            log.info(
                    "✓ Basic authentication failure test passed - exception: {}",
                    exception.getMessage());
        }
    }

    // ==================== API Key Authentication Tests ====================

    /** Test successful API key authentication with valid key */
    @Test
    public void testApiKeyAuthenticationSuccess() throws Exception {
        log.info("=== Testing API Key Authentication Success ===");

        Map<String, Object> config = createApiKeyConfig(validApiKeyId, validApiKeySecret);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "api_key", provider.getAuthType(), "Provider should be api_key auth type");

        // Test client creation and functionality
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");

            // Verify client can perform operations with real API key
            long docCount = client.getIndexDocsCount(TEST_INDEX).get(0).getDocsCount();
            Assertions.assertTrue(docCount > 0, "Should be able to query index with valid API key");

            log.info("✓ API key authentication success test passed - {} documents found", docCount);
        }
    }

    /** Test API key authentication failure with invalid key */
    @Test
    public void testApiKeyAuthenticationFailure() throws Exception {
        log.info("=== Testing API Key Authentication Failure ===");

        Map<String, Object> config = createApiKeyConfig(INVALID_API_KEY_ID, INVALID_API_KEY_SECRET);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation (should succeed)
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "api_key", provider.getAuthType(), "Provider should be api_key auth type");

        // Test client creation (should succeed)
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created");

            // Test operation (should fail with authentication error)
            Exception exception =
                    Assertions.assertThrows(
                            Exception.class,
                            () -> {
                                client.getIndexDocsCount(TEST_INDEX);
                            },
                            "Should throw exception when using invalid API key");

            log.info(
                    "✓ API key authentication failure test passed - exception: {}",
                    exception.getMessage());
        }
    }

    /** Test API key authentication with encoded format */
    @Test
    public void testApiKeyEncodedAuthentication() throws Exception {
        log.info("=== Testing API Key Encoded Authentication ===");

        Map<String, Object> config = createApiKeyEncodedConfig(validEncodedApiKey);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        // Test provider creation
        AuthenticationProvider provider =
                AuthenticationProviderFactory.createProvider(readonlyConfig);
        Assertions.assertNotNull(provider, "Authentication provider should be created");
        Assertions.assertEquals(
                "api_key_encoded",
                provider.getAuthType(),
                "Provider should be api_key_encoded auth type");

        // Test client creation and functionality
        try (EsRestClient client = EsRestClient.createInstance(readonlyConfig)) {
            Assertions.assertNotNull(client, "EsRestClient should be created successfully");

            // Verify client can perform operations with encoded API key
            long docCount = client.getIndexDocsCount(TEST_INDEX).get(0).getDocsCount();
            Assertions.assertTrue(
                    docCount > 0, "Should be able to query index with valid encoded API key");

            log.info("✓ API key encoded authentication test passed - {} documents found", docCount);
        }
    }

    /** E2E test: API Key authentication source and sink */
    @TestTemplate
    public void testE2EApiKeyAuthSourceAndSink(TestContainer container) throws Exception {
        log.info("=== E2E Test: API Key Authentication Source and Sink ===");

        // Setup test data
        setupAuthTestData();

        // Create temporary config file with real API key values in resources directory
        String configContent = createApiKeyConfigContent();
        java.io.File resourcesDir = new java.io.File("src/test/resources/elasticsearch");
        if (!resourcesDir.exists()) {
            resourcesDir.mkdirs();
        }

        java.io.File tempConfigFile =
                new java.io.File(resourcesDir, "elasticsearch_auth_apikey_temp.conf");
        try (java.io.FileWriter writer = new java.io.FileWriter(tempConfigFile)) {
            writer.write(configContent);
        }

        try {
            // Execute SeaTunnel job with API key auth using relative path
            Container.ExecResult execResult =
                    container.executeJob("/elasticsearch/elasticsearch_auth_apikey_temp.conf");
            Assertions.assertEquals(
                    0, execResult.getExitCode(), "Job should complete successfully");

            // Wait for index refresh
            Thread.sleep(2000);

            // Verify results
            long targetCount =
                    esRestClient.getIndexDocsCount("auth_test_apikey_target").get(0).getDocsCount();
            log.info("✓ API Key auth E2E test completed - {} documents processed", targetCount);
            Assertions.assertTrue(
                    targetCount > 0, "Should have processed documents with API key auth");

        } finally {
            // Clean up temporary file
            if (tempConfigFile.exists()) {
                tempConfigFile.delete();
            }
        }
    }

    /** E2E test: API Key Encoded authentication source and sink */
    @TestTemplate
    public void testE2EApiKeyEncodedAuthSourceAndSink(TestContainer container) throws Exception {
        log.info("=== E2E Test: API Key Encoded Authentication Source and Sink ===");

        // Setup test data
        setupAuthTestData();

        // Create temporary config file with real encoded API key values
        String configContent = createApiKeyEncodedConfigContent();
        java.io.File resourcesDir = new java.io.File("src/test/resources/elasticsearch");
        if (!resourcesDir.exists()) {
            resourcesDir.mkdirs();
        }

        java.io.File tempConfigFile =
                new java.io.File(resourcesDir, "elasticsearch_auth_apikey_encoded_temp.conf");
        try (java.io.FileWriter writer = new java.io.FileWriter(tempConfigFile)) {
            writer.write(configContent);
        }

        try {
            // Execute SeaTunnel job with encoded API key auth
            Container.ExecResult execResult =
                    container.executeJob(
                            "/elasticsearch/elasticsearch_auth_apikey_encoded_temp.conf");
            Assertions.assertEquals(
                    0, execResult.getExitCode(), "Job should complete successfully");

            // Wait for index refresh
            Thread.sleep(2000);

            // Verify results
            long targetCount =
                    esRestClient
                            .getIndexDocsCount("auth_test_apikey_encoded_target")
                            .get(0)
                            .getDocsCount();
            log.info(
                    "✓ API Key Encoded auth E2E test completed - {} documents processed",
                    targetCount);
            Assertions.assertTrue(
                    targetCount > 0, "Should have processed documents with encoded API key auth");

        } finally {
            // Clean up temporary file
            if (tempConfigFile.exists()) {
                tempConfigFile.delete();
            }
        }
    }

    /** Create API Key configuration content with real values */
    private String createApiKeyConfigContent() {
        return String.format(
                "env {\n"
                        + "  parallelism = 1\n"
                        + "  job.mode = \"BATCH\"\n"
                        + "}\n"
                        + "\n"
                        + "source {\n"
                        + "  Elasticsearch {\n"
                        + "    hosts = [\"https://elasticsearch:9200\"]\n"
                        + "    auth_type = \"api_key\"\n"
                        + "    auth.api_key_id = \"%s\"\n"
                        + "    auth.api_key = \"%s\"\n"
                        + "    tls_verify_certificate = false\n"
                        + "    tls_verify_hostname = false\n"
                        + "\n"
                        + "    index = \"auth_test_index\"\n"
                        + "    query = {\"match_all\": {}}\n"
                        + "    schema = {\n"
                        + "      fields {\n"
                        + "        id = int\n"
                        + "        name = string\n"
                        + "        category = string\n"
                        + "        price = double\n"
                        + "        timestamp = timestamp\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n"
                        + "\n"
                        + "sink {\n"
                        + "  Elasticsearch {\n"
                        + "    hosts = [\"https://elasticsearch:9200\"]\n"
                        + "    auth_type = \"api_key\"\n"
                        + "    auth.api_key_id = \"%s\"\n"
                        + "    auth.api_key = \"%s\"\n"
                        + "    tls_verify_certificate = false\n"
                        + "    tls_verify_hostname = false\n"
                        + "\n"
                        + "    index = \"auth_test_apikey_target\"\n"
                        + "    schema_save_mode = \"CREATE_SCHEMA_WHEN_NOT_EXIST\"\n"
                        + "    data_save_mode = \"APPEND_DATA\"\n"
                        + "  }\n"
                        + "}\n",
                validApiKeyId, validApiKeySecret, validApiKeyId, validApiKeySecret);
    }

    /** Create API Key Encoded configuration content with real values */
    private String createApiKeyEncodedConfigContent() {
        return String.format(
                "env {\n"
                        + "  parallelism = 1\n"
                        + "  job.mode = \"BATCH\"\n"
                        + "}\n"
                        + "\n"
                        + "source {\n"
                        + "  Elasticsearch {\n"
                        + "    hosts = [\"https://elasticsearch:9200\"]\n"
                        + "    auth_type = \"api_key_encoded\"\n"
                        + "    auth.api_key_encoded = \"%s\"\n"
                        + "    tls_verify_certificate = false\n"
                        + "    tls_verify_hostname = false\n"
                        + "\n"
                        + "    index = \"auth_test_index\"\n"
                        + "    query = {\"match_all\": {}}\n"
                        + "    schema = {\n"
                        + "      fields {\n"
                        + "        id = int\n"
                        + "        name = string\n"
                        + "        category = string\n"
                        + "        price = double\n"
                        + "        timestamp = timestamp\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n"
                        + "\n"
                        + "sink {\n"
                        + "  Elasticsearch {\n"
                        + "    hosts = [\"https://elasticsearch:9200\"]\n"
                        + "    auth_type = \"api_key_encoded\"\n"
                        + "    auth.api_key_encoded = \"%s\"\n"
                        + "    tls_verify_certificate = false\n"
                        + "    tls_verify_hostname = false\n"
                        + "\n"
                        + "    index = \"auth_test_apikey_encoded_target\"\n"
                        + "    schema_save_mode = \"CREATE_SCHEMA_WHEN_NOT_EXIST\"\n"
                        + "    data_save_mode = \"APPEND_DATA\"\n"
                        + "  }\n"
                        + "}\n",
                validEncodedApiKey, validEncodedApiKey);
    }

    /** Setup test data for authentication tests */
    private void setupAuthTestData() throws Exception {
        String testIndex = "auth_test_index";

        // Create index mapping
        String mapping =
                "{"
                        + "\"mappings\": {"
                        + "\"properties\": {"
                        + "\"id\": {\"type\": \"integer\"},"
                        + "\"name\": {\"type\": \"text\"},"
                        + "\"category\": {\"type\": \"keyword\"},"
                        + "\"price\": {\"type\": \"double\"},"
                        + "\"timestamp\": {\"type\": \"date\"}"
                        + "}"
                        + "}"
                        + "}";

        try {
            esRestClient.createIndex(testIndex, mapping);
            log.info("Created test index: {}", testIndex);
        } catch (Exception e) {
            log.warn("Index might already exist: {}", e.getMessage());
        }

        // Insert test data
        StringBuilder requestBody = new StringBuilder();
        String indexHeader = "{\"index\":{\"_index\":\"" + testIndex + "\"}}\n";

        String[] categories = {"electronics", "books", "clothing", "home", "sports"};
        for (int i = 1; i <= 10; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("id", i);
            doc.put("name", "Auth Test Product " + i);
            doc.put("category", categories[i % categories.length]);
            doc.put("price", 15.99 + (i * 3.5)); // Prices from 19.49 to 50.49
            doc.put("timestamp", "2024-01-" + String.format("%02d", i) + "T10:00:00Z");

            requestBody.append(indexHeader);
            requestBody.append(objectMapper.writeValueAsString(doc));
            requestBody.append("\n");
        }

        BulkResponse response = esRestClient.bulk(requestBody.toString());
        if (response.isErrors()) {
            log.warn("Some documents might already exist: {}", response.getResponse());
        }

        // Wait for index refresh
        Thread.sleep(2000);

        long docCount = esRestClient.getIndexDocsCount(testIndex).get(0).getDocsCount();
        log.info("Test data setup completed - {} documents in source index", docCount);
    }
}
