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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;
import org.apache.seatunnel.api.options.table.FieldOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileDiscoveryMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3FileSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3HadoopConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;

final class S3SourceDryRunValidator {
    private static final int NETWORK_TIMEOUT_MILLIS = 5000;

    private S3SourceDryRunValidator() {}

    static void validateSchemaOptions(ReadonlyConfig options) {
        if (options.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()
                || !options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires a single table with an explicit inline schema.");
        }
        Map<String, Object> schema = options.get(ConnectorCommonOptions.SCHEMA);
        if (schema.containsKey(ColumnOptions.METADATA_TABLE_ID.key())
                || (!options.getOptional(FieldOptions.FIELDS).isPresent()
                        && !schema.containsKey(ColumnOptions.COLUMNS.key()))) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires schema.fields or schema.columns, not metadata schema lookup.");
        }
        if (!Arrays.asList(FileFormat.TEXT, FileFormat.CSV, FileFormat.JSON, FileFormat.XML)
                .contains(options.get(FileBaseSourceOptions.FILE_FORMAT_TYPE))) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run supports text, csv, json and xml without reading file contents.");
        }
        if (options.get(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH)
                || options.getOptional(FileBaseSourceOptions.READ_COLUMNS).isPresent()) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires parse_partition_from_path=false and no read_columns to preserve the runtime schema.");
        }
    }

    static void validate(ReadonlyConfig options) throws IOException {
        validateSchemaOptions(options);
        URI bucket = bucketUri(options);
        Path path = sourcePath(options, bucket);
        Configuration configuration = validationConfiguration(options, bucket);
        // Validate pure SDK settings before Hadoop creates any credential providers.
        DefaultS3ClientFactory.createAwsConf(configuration);
        try (DryRunClientFactory factory = new DryRunClientFactory()) {
            factory.setConf(configuration);
            AmazonS3 client = factory.createS3Client(bucket);
            validatePath(
                    client,
                    bucket.getHost(),
                    path,
                    options.get(FileBaseSourceOptions.DISCOVERY_MODE),
                    configuration.getInt(Constants.LIST_VERSION, Constants.DEFAULT_LIST_VERSION));
        }
    }

    static URI bucketUri(ReadonlyConfig options) {
        URI bucket;
        try {
            bucket = URI.create(options.get(S3FileSourceOptions.S3_BUCKET));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires an s3a://bucket URI.");
        }
        if (!"s3a".equals(bucket.getScheme())
                || bucket.getHost() == null
                || bucket.getUserInfo() != null
                || bucket.getPort() != -1
                || bucket.getQuery() != null
                || bucket.getFragment() != null
                || (!bucket.getPath().isEmpty() && !"/".equals(bucket.getPath()))) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires an s3a://bucket URI.");
        }
        return bucket;
    }

    static Path sourcePath(ReadonlyConfig options, URI bucket) {
        Path path;
        try {
            path = new Path(options.get(FileBaseSourceOptions.FILE_PATH));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires an absolute path in the configured bucket.");
        }
        URI uri = path.toUri();
        if (!path.isAbsolute()
                || (uri.getScheme() != null && !"s3a".equals(uri.getScheme()))
                || (uri.getAuthority() != null
                        && !bucket.getAuthority().equals(uri.getAuthority()))) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires an absolute path in the configured bucket.");
        }
        return path;
    }

    static Configuration validationConfiguration(ReadonlyConfig options, URI bucket) {
        HadoopConf hadoopConf = S3HadoopConf.buildWithReadOnlyConfig(options);
        Configuration configuration = hadoopConf.toConfiguration();
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        configuration = S3AUtils.propagateBucketOptions(configuration, bucket.getHost());
        if (configuration.getBoolean(Constants.PURGE_EXISTING_MULTIPART, false)
                || !Constants.S3GUARD_METASTORE_NULL.equals(
                        configuration.get(
                                Constants.S3_METADATA_STORE_IMPL, Constants.S3GUARD_METASTORE_NULL))
                || !Constants.DEFAULT_S3_CLIENT_FACTORY_IMPL
                        .getName()
                        .equals(
                                configuration.get(
                                        Constants.S3_CLIENT_FACTORY_IMPL,
                                        Constants.DEFAULT_S3_CLIENT_FACTORY_IMPL.getName()))) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run does not support multipart purge, S3Guard or a custom S3 client factory.");
        }
        if (!configuration.getTrimmed(Constants.S3A_SECURITY_CREDENTIAL_PROVIDER_PATH, "").isEmpty()
                || "SSE-C"
                        .equalsIgnoreCase(
                                configuration.getTrimmed(
                                        Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM, ""))) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run does not support SSE-C or fs.s3a.security.credential.provider.path.");
        }
        clampTimeout(configuration, bucket, Constants.ESTABLISH_TIMEOUT);
        clampTimeout(configuration, bucket, Constants.SOCKET_TIMEOUT);
        for (String key :
                Arrays.asList(
                        Constants.MAX_ERROR_RETRIES,
                        Constants.RETRY_LIMIT,
                        Constants.RETRY_THROTTLE_LIMIT)) {
            S3AUtils.clearBucketOption(configuration, bucket.getHost(), key);
            configuration.setInt(key, 0);
        }
        return configuration;
    }

    private static void clampTimeout(Configuration configuration, URI bucket, String key) {
        int configured = configuration.getInt(key, NETWORK_TIMEOUT_MILLIS);
        if (configured < 0) {
            throw new IllegalArgumentException(
                    "S3File connect dry-run requires non-negative " + key);
        }
        S3AUtils.clearBucketOption(configuration, bucket.getHost(), key);
        configuration.setInt(
                key,
                configured == 0
                        ? NETWORK_TIMEOUT_MILLIS
                        : Math.min(configured, NETWORK_TIMEOUT_MILLIS));
    }

    static void validatePath(
            AmazonS3 client,
            String bucket,
            Path path,
            FileDiscoveryMode discoveryMode,
            int listVersion)
            throws IOException {
        String key = path.toUri().getPath().substring(1);
        if (!key.isEmpty()) {
            try {
                client.getObjectMetadata(bucket, key);
                return;
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
            }
        }
        String prefix = key.isEmpty() || key.endsWith("/") ? key : key + "/";
        boolean empty;
        if (listVersion == 1) {
            ObjectListing result =
                    client.listObjects(
                            new ListObjectsRequest()
                                    .withBucketName(bucket)
                                    .withPrefix(prefix)
                                    .withDelimiter("/")
                                    .withMaxKeys(1));
            empty = result.getObjectSummaries().isEmpty() && result.getCommonPrefixes().isEmpty();
        } else {
            ListObjectsV2Result result =
                    client.listObjectsV2(
                            new ListObjectsV2Request()
                                    .withBucketName(bucket)
                                    .withPrefix(prefix)
                                    .withDelimiter("/")
                                    .withMaxKeys(1));
            empty = result.getObjectSummaries().isEmpty() && result.getCommonPrefixes().isEmpty();
        }
        // Only a successful empty listing proves an accessible prefix awaiting its first file.
        if (empty && !key.isEmpty() && discoveryMode != FileDiscoveryMode.CONTINUOUS) {
            throw new FileNotFoundException(
                    "S3File connect dry-run could not find the configured batch source path.");
        }
    }

    private static final class DryRunClientFactory extends DefaultS3ClientFactory
            implements AutoCloseable {
        private AmazonS3 client;
        private AWSCredentialProviderList credentials;

        @Override
        protected AmazonS3 newAmazonS3Client(
                AWSCredentialsProvider credentials, ClientConfiguration configuration) {
            this.credentials = (AWSCredentialProviderList) credentials;
            this.client = super.newAmazonS3Client(credentials, configuration);
            return client;
        }

        @Override
        public void close() {
            try {
                if (client != null) {
                    client.shutdown();
                }
            } finally {
                if (credentials != null) {
                    credentials.close();
                }
            }
        }
    }
}
