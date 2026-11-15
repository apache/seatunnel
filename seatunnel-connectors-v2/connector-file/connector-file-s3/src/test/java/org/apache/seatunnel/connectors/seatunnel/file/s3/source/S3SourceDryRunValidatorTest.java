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

package org.apache.seatunnel.connectors.seatunnel.file.s3.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileDiscoveryMode;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3FileSourceOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class S3SourceDryRunValidatorTest {
    private static final URI BUCKET = URI.create("s3a://warehouse");

    @ParameterizedTest
    @ValueSource(strings = {"text", "csv", "json", "xml"})
    void shouldReturnConfiguredSchemaWithoutCreatingSource(String format) {
        Map<String, Object> config = sourceConfig();
        config.put("file_format_type", format);
        TableSourceFactoryContext context = context(config);
        S3FileSourceFactory factory = new S3FileSourceFactory();

        List<CatalogTable> tables = factory.inferSchemaForDryRun(context);

        assertEquals(1, tables.size());
        assertEquals(
                factory.discoverTableSchemas(context).get(0).getSeaTunnelRowType(),
                tables.get(0).getSeaTunnelRowType());
        assertEquals("event_id", tables.get(0).getSeaTunnelRowType().getFieldName(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "markdown", "pdf", "parquet", "orc", "excel"})
    void shouldRefuseFormatsRequiringDifferentSchemaHandling(String format) {
        Map<String, Object> config = sourceConfig();
        config.put("file_format_type", format);
        assertSchemaFailure(config, "supports text, csv, json and xml");
    }

    @Test
    void shouldRefuseMissingSchema() {
        Map<String, Object> config = sourceConfig();
        config.remove("schema");
        assertSchemaFailure(config, "explicit inline schema");
    }

    @Test
    void shouldRefuseMetadataSchemaLookup() {
        Map<String, Object> config = sourceConfig();
        config.put("schema", Collections.singletonMap("metadata_table_id", "warehouse.events"));
        assertSchemaFailure(config, "not metadata schema lookup");
    }

    @Test
    void shouldRefuseTableConfigs() {
        Map<String, Object> config = sourceConfig();
        config.put("tables_configs", Collections.singletonList(sourceConfig()));
        assertSchemaFailure(config, "single table");
    }

    @Test
    void shouldRefuseDefaultPartitionInference() {
        Map<String, Object> config = sourceConfig();
        config.remove("parse_partition_from_path");
        assertSchemaFailure(config, "parse_partition_from_path=false");
    }

    @Test
    void shouldRefuseProjection() {
        Map<String, Object> config = sourceConfig();
        config.put("read_columns", Collections.singletonList("event_id"));
        assertSchemaFailure(config, "no read_columns");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s3n://warehouse",
                "s3a://warehouse/folder",
                "s3a://secret@warehouse",
                "s3a://warehouse?key=secret"
            })
    void shouldRefuseUnsupportedBucketUriWithoutEchoingIt(String bucket) {
        Map<String, Object> config = sourceConfig();
        config.put("bucket", bucket);
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> S3SourceDryRunValidator.bucketUri(ReadonlyConfig.fromMap(config)));
        assertTrue(failure.getMessage().contains("s3a://bucket URI"));
        assertFalse(failure.getMessage().contains("secret"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "events",
                "s3a://other/events",
                "file:///events",
                "s3a://secret@warehouse/events",
                "s3a://[secret"
            })
    void shouldRefusePathOutsideConfiguredBucket(String path) {
        Map<String, Object> config = sourceConfig();
        config.put("path", path);
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                S3SourceDryRunValidator.sourcePath(
                                        ReadonlyConfig.fromMap(config), BUCKET));
        assertTrue(failure.getMessage().contains("absolute path in the configured bucket"));
        assertFalse(failure.getMessage().contains("secret"));
    }

    @Test
    void shouldRetainEndpointAndCredentialsWithoutChangingInputProperties() {
        Map<String, Object> config = sourceConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put(Constants.PATH_STYLE_ACCESS, "true");
        properties.put(Constants.SOCKET_TIMEOUT, "1200");
        config.put("hadoop_s3_properties", properties);

        Configuration configuration = configuration(config);

        assertEquals("http://localhost:9000", configuration.get(Constants.ENDPOINT));
        assertEquals("access-key", configuration.get(Constants.ACCESS_KEY));
        assertEquals("secret-key", configuration.get(Constants.SECRET_KEY));
        assertEquals(
                S3FileSourceOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER,
                configuration.get(Constants.AWS_CREDENTIALS_PROVIDER));
        assertEquals(1200, configuration.getInt(Constants.SOCKET_TIMEOUT, -1));
        assertTrue(configuration.getBoolean(Constants.PATH_STYLE_ACCESS, false));
        assertEquals(2, properties.size());
        assertEquals("1200", properties.get(Constants.SOCKET_TIMEOUT));
    }

    @Test
    void shouldBoundBucketSpecificTimeoutsAndRetriesAfterS3AReappliesOverrides() {
        Map<String, Object> config = sourceConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.s3a.bucket.warehouse.connection.timeout", "0");
        properties.put("fs.s3a.bucket.warehouse.connection.establish.timeout", "2000");
        properties.put("fs.s3a.bucket.warehouse.attempts.maximum", "99");
        properties.put("fs.s3a.bucket.warehouse.retry.limit", "99");
        properties.put("fs.s3a.bucket.warehouse.retry.throttle.limit", "99");
        config.put("hadoop_s3_properties", properties);

        Configuration configuration =
                S3AUtils.propagateBucketOptions(configuration(config), "warehouse");

        assertEquals(5000, configuration.getInt(Constants.SOCKET_TIMEOUT, -1));
        assertEquals(2000, configuration.getInt(Constants.ESTABLISH_TIMEOUT, -1));
        assertEquals(0, configuration.getInt(Constants.MAX_ERROR_RETRIES, -1));
        assertEquals(0, configuration.getInt(Constants.RETRY_LIMIT, -1));
        assertEquals(0, configuration.getInt(Constants.RETRY_THROTTLE_LIMIT, -1));
        assertEquals("99", properties.get("fs.s3a.bucket.warehouse.retry.limit"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"fs.s3a.multipart.purge", "fs.s3a.bucket.warehouse.multipart.purge"})
    void shouldRefuseMultipartPurgeBeforeOpeningFilesystem(String key) {
        assertConfigurationFailure(key, "true", "multipart purge");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"fs.s3a.metadatastore.impl", "fs.s3a.bucket.warehouse.metadatastore.impl"})
    void shouldRefuseMetadataStoreInitialization(String key) {
        assertConfigurationFailure(key, Constants.S3GUARD_METASTORE_DYNAMO, "S3Guard");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "fs.s3a.s3.client.factory.impl",
                "fs.s3a.bucket.warehouse.s3.client.factory.impl"
            })
    void shouldRefuseCustomClientInitialization(String key) {
        assertConfigurationFailure(key, "example.CustomClientFactory", "custom S3 client factory");
    }

    @Test
    void shouldRefuseNegativeTimeout() {
        assertConfigurationFailure(Constants.SOCKET_TIMEOUT, "-1", "non-negative");
    }

    @Test
    void shouldCheckOnlyPathMetadata() throws IOException {
        AmazonS3 client = mock(AmazonS3.class);
        Path path = new Path("/events");
        when(client.getObjectMetadata("warehouse", "events")).thenReturn(new ObjectMetadata());
        S3SourceDryRunValidator.validatePath(client, "warehouse", path, FileDiscoveryMode.ONCE, 2);
        verify(client).getObjectMetadata("warehouse", "events");
        verifyNoMoreInteractions(client);
    }

    @Test
    void shouldFailForMissingBatchPath() throws IOException {
        AmazonS3 client = emptyPrefixClient();
        Path path = new Path("/events");
        FileNotFoundException failure =
                assertThrows(
                        FileNotFoundException.class,
                        () ->
                                S3SourceDryRunValidator.validatePath(
                                        client, "warehouse", path, FileDiscoveryMode.ONCE, 2));
        assertTrue(failure.getMessage().contains("batch source path"));
    }

    @Test
    void shouldAllowEmptyContinuousPrefix() throws IOException {
        AmazonS3 client = emptyPrefixClient();
        Path path = new Path("/events");
        assertDoesNotThrow(
                () ->
                        S3SourceDryRunValidator.validatePath(
                                client, "warehouse", path, FileDiscoveryMode.CONTINUOUS, 2));
    }

    @Test
    void shouldPropagateDeniedAccessForContinuousDiscovery() throws IOException {
        AmazonS3 client = emptyPrefixClient();
        Path path = new Path("/events");
        AmazonS3Exception denied = serviceFailure(403);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(denied);
        AmazonS3Exception failure =
                assertThrows(
                        AmazonS3Exception.class,
                        () ->
                                S3SourceDryRunValidator.validatePath(
                                        client,
                                        "warehouse",
                                        path,
                                        FileDiscoveryMode.CONTINUOUS,
                                        2));
        assertSame(denied, failure);
        assertTrue(failure.getMessage().contains("metadata failure"));
    }

    @Test
    void shouldFailForMissingBucketDuringContinuousDiscovery() {
        AmazonS3 client = emptyPrefixClient();
        AmazonS3Exception missing = serviceFailure(404);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(missing);
        AmazonS3Exception failure =
                assertThrows(
                        AmazonS3Exception.class,
                        () ->
                                S3SourceDryRunValidator.validatePath(
                                        client,
                                        "warehouse",
                                        new Path("/events"),
                                        FileDiscoveryMode.CONTINUOUS,
                                        2));
        assertSame(missing, failure);
        assertEquals(404, failure.getStatusCode());
    }

    @Test
    void shouldBoundPrefixListingWithoutFollowingContinuation() throws IOException {
        AmazonS3 client = emptyPrefixClient();
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(
                        invocation -> {
                            ListObjectsV2Request request = invocation.getArgument(0);
                            assertEquals("warehouse", request.getBucketName());
                            assertEquals("events/", request.getPrefix());
                            assertEquals("/", request.getDelimiter());
                            assertEquals(1, request.getMaxKeys());
                            ListObjectsV2Result result = new ListObjectsV2Result();
                            result.getObjectSummaries().add(new S3ObjectSummary());
                            result.setTruncated(true);
                            result.setNextContinuationToken("more-events");
                            return result;
                        });
        S3SourceDryRunValidator.validatePath(
                client, "warehouse", new Path("/events"), FileDiscoveryMode.ONCE, 2);
        verify(client).getObjectMetadata("warehouse", "events");
        verify(client).listObjectsV2(any(ListObjectsV2Request.class));
        verifyNoMoreInteractions(client);
    }

    @Test
    void shouldRefuseCustomerProvidedEncryptionKeys() {
        assertConfigurationFailure(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM, "SSE-C", "SSE-C");
    }

    @Test
    void shouldHonorLegacyListVersion() throws IOException {
        AmazonS3 client = mock(AmazonS3.class);
        when(client.getObjectMetadata("warehouse", "events")).thenThrow(serviceFailure(404));
        when(client.listObjects(any(ListObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            ListObjectsRequest request = invocation.getArgument(0);
                            assertEquals(Integer.valueOf(1), request.getMaxKeys());
                            assertEquals("events/", request.getPrefix());
                            assertEquals("/", request.getDelimiter());
                            return new ObjectListing();
                        });
        S3SourceDryRunValidator.validatePath(
                client, "warehouse", new Path("/events"), FileDiscoveryMode.CONTINUOUS, 1);
        verify(client).getObjectMetadata("warehouse", "events");
        verify(client).listObjects(any(ListObjectsRequest.class));
        verifyNoMoreInteractions(client);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3})
    void shouldMatchS3AFallbackForOtherListVersions(int listVersion) throws IOException {
        AmazonS3 client = emptyPrefixClient();
        S3SourceDryRunValidator.validatePath(
                client,
                "warehouse",
                new Path("/events"),
                FileDiscoveryMode.CONTINUOUS,
                listVersion);
        verify(client).getObjectMetadata("warehouse", "events");
        verify(client).listObjectsV2(any(ListObjectsV2Request.class));
        verifyNoMoreInteractions(client);
    }

    @Test
    void shouldRefuseS3SpecificCredentialStoreBeforeClientInitialization() {
        assertConfigurationFailure(
                Constants.S3A_SECURITY_CREDENTIAL_PROVIDER_PATH,
                "jceks://file/credentials",
                "credential.provider.path");
    }

    private static AmazonS3 emptyPrefixClient() {
        AmazonS3 client = mock(AmazonS3.class);
        when(client.getObjectMetadata("warehouse", "events")).thenThrow(serviceFailure(404));
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(new ListObjectsV2Result());
        return client;
    }

    @Test
    void shouldReturnExplicitColumnSchema() {
        Map<String, Object> config = sourceConfig();
        Map<String, Object> column = new HashMap<>();
        column.put("name", "event_id");
        column.put("type", "bigint");
        config.put(
                "schema", Collections.singletonMap("columns", Collections.singletonList(column)));
        CatalogTable table = new S3FileSourceFactory().inferSchemaForDryRun(context(config)).get(0);
        assertEquals("event_id", table.getSeaTunnelRowType().getFieldName(0));
    }

    @Test
    void shouldRetainDefaultCredentialProvider() {
        Map<String, Object> config = sourceConfig();
        config.remove("fs.s3a.aws.credentials.provider");
        Configuration configuration = configuration(config);
        assertEquals(
                S3FileSourceOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER,
                configuration.get(Constants.AWS_CREDENTIALS_PROVIDER));
    }

    private static AmazonS3Exception serviceFailure(int status) {
        AmazonS3Exception failure = new AmazonS3Exception("metadata failure");
        failure.setStatusCode(status);
        return failure;
    }

    static Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("path", "/events");
        config.put("file_format_type", "json");
        config.put("bucket", BUCKET.toString());
        config.put("fs.s3a.endpoint", "http://localhost:9000");
        config.put(
                "fs.s3a.aws.credentials.provider",
                S3FileSourceOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER);
        config.put("access_key", "access-key");
        config.put("secret_key", "secret-key");
        config.put("parse_partition_from_path", false);
        config.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("event_id", "bigint")));
        return config;
    }

    private static TableSourceFactoryContext context(Map<String, Object> config) {
        return new TableSourceFactoryContext(
                ReadonlyConfig.fromMap(config), S3SourceDryRunValidatorTest.class.getClassLoader());
    }

    private static Configuration configuration(Map<String, Object> config) {
        return S3SourceDryRunValidator.validationConfiguration(
                ReadonlyConfig.fromMap(config), BUCKET);
    }

    private static void assertSchemaFailure(Map<String, Object> config, String message) {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new S3FileSourceFactory().inferSchemaForDryRun(context(config)));
        assertTrue(failure.getMessage().contains(message), failure.getMessage());
    }

    private static void assertConfigurationFailure(String key, String value, String message) {
        Map<String, Object> config = sourceConfig();
        config.put("hadoop_s3_properties", Collections.singletonMap(key, value));
        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> configuration(config));
        assertTrue(failure.getMessage().contains(message), failure.getMessage());
    }
}
