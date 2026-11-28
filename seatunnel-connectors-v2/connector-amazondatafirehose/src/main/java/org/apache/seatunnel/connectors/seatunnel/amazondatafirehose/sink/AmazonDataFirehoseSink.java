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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink.AmazonDataFirehoseSinkOptions.Format;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Validates one DirectPut destination and creates a worker-owned buffered Firehose writer.
 * Credential and format settings are additive; external acceptance cannot be rolled back.
 */
final class AmazonDataFirehoseSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private final CatalogTable catalog;
    private final String stream;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String token;
    private final URI endpoint;
    private final Format format;
    private final String delimiter;
    private final int maxRecordBytes;
    private final int batchSize;
    private final int timeoutMillis;
    private final int maxRetries;

    /*
     * Reject invalid origins, credential pairs and record limits before creating network resources.
     * The catalog supplies the native row schema; encoded formats require exactly one payload field.
     */
    AmazonDataFirehoseSink(ReadonlyConfig options, CatalogTable catalog) {
        this.catalog = catalog;
        this.stream = options.get(AmazonDataFirehoseSinkOptions.DELIVERY_STREAM);
        if (!stream.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw invalid("delivery_stream must be a valid Firehose stream name");
        }
        this.region = options.get(AmazonDataFirehoseSinkOptions.REGION);
        if (region.length() > 64 || !region.matches("[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+")) {
            throw invalid("region must be an AWS Region identifier");
        }
        this.accessKey = options.get(AmazonDataFirehoseSinkOptions.ACCESS_KEY);
        this.secretKey = options.get(AmazonDataFirehoseSinkOptions.SECRET_KEY);
        this.token = options.get(AmazonDataFirehoseSinkOptions.TOKEN);
        if ((accessKey == null) != (secretKey == null) || (token != null && accessKey == null)) {
            throw invalid(
                    "provide both access_key and secret_key when supplying explicit credentials");
        }
        if (accessKey != null
                && (!accessKey.matches("[A-Za-z0-9]{1,128}")
                        || !validSecret(secretKey)
                        || (token != null && !validSecret(token)))) {
            throw invalid("explicit AWS credentials are invalid");
        }
        this.endpoint =
                parseEndpoint(
                        options.get(AmazonDataFirehoseSinkOptions.ENDPOINT_URL),
                        options.get(AmazonDataFirehoseSinkOptions.ALLOW_INSECURE_HTTP));
        this.format = options.get(AmazonDataFirehoseSinkOptions.FORMAT);
        this.delimiter = options.get(AmazonDataFirehoseSinkOptions.FIELD_DELIMITER);
        if (format == Format.DELIMITED
                && (delimiter.length() != 1
                        || delimiter.charAt(0) == '"'
                        || delimiter.charAt(0) == '\n'
                        || delimiter.charAt(0) == '\r'
                        || Character.isSurrogate(delimiter.charAt(0)))) {
            throw invalid("field_delimiter must be one character other than a quote or line break");
        }
        this.maxRecordBytes =
                requireRange(
                        options.get(AmazonDataFirehoseSinkOptions.MAX_RECORD_BYTES),
                        1,
                        1000 * 1024,
                        "max_record_bytes");
        this.batchSize =
                requireRange(
                        options.get(AmazonDataFirehoseSinkOptions.BATCH_SIZE),
                        1,
                        500,
                        "batch_size");
        this.timeoutMillis =
                requireRange(
                        options.get(AmazonDataFirehoseSinkOptions.BATCH_TIMEOUT_MS),
                        1,
                        300000,
                        "batch_timeout_ms");
        this.maxRetries =
                requireRange(
                        options.get(AmazonDataFirehoseSinkOptions.MAX_RETRIES),
                        0,
                        10,
                        "max_retries");

        SeaTunnelRowType type = catalog.getSeaTunnelRowType();
        if (type.getTotalFields() == 0) {
            throw invalid("requires a nonempty input schema");
        }
        if ((format == Format.RAW || format == Format.JSON_LINES)
                && (type.getTotalFields() != 1
                        || !"payload".equals(type.getFieldName(0))
                        || (!BasicType.STRING_TYPE.equals(type.getFieldType(0))
                                && (format != Format.RAW
                                        || !PrimitiveByteArrayType.INSTANCE.equals(
                                                type.getFieldType(0)))))) {
            throw invalid("requires one payload STRING column, or a payload BYTES column for RAW");
        }
    }

    @Override
    public String getPluginName() {
        return "AmazonDataFirehose";
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalog);
    }

    /*
     * Resolve worker credentials without profile/metadata discovery and bound native conversion work.
     * The writer owns its client; engine flush signals stay on the ordinary writer execution path.
     */
    @Override
    public AmazonDataFirehoseSinkWriter createWriter(SinkWriter.Context context) {
        AwsCredentialsProvider credentials;
        if (accessKey == null) {
            credentials = EnvironmentVariableCredentialsProvider.create();
        } else {
            credentials =
                    StaticCredentialsProvider.create(
                            token == null
                                    ? AwsBasicCredentials.create(accessKey, secretKey)
                                    : AwsSessionCredentials.create(accessKey, secretKey, token));
        }
        SeaTunnelRowType rowType = catalog.getSeaTunnelRowType();
        FirehoseRowSerializationSchema serializer =
                format == Format.JSON || format == Format.DELIMITED
                        ? new FirehoseRowSerializationSchema(
                                rowType, format, delimiter, maxRecordBytes)
                        : null;
        FirehoseBatchClient client =
                new FirehoseBatchClient(
                        stream, region, endpoint, credentials, timeoutMillis, maxRetries);
        AmazonDataFirehoseSinkWriter writer =
                new AmazonDataFirehoseSinkWriter(
                        client,
                        format,
                        serializer,
                        rowType.getTotalFields(),
                        batchSize,
                        maxRecordBytes);
        context.enableDeferredTerminalWriteOutcomes();
        context.registerFlushAction(writer::flush);
        return writer;
    }

    private static boolean validSecret(String secret) {
        return secret.length() > 0
                && secret.length() <= 8192
                && secret.chars().allMatch(c -> c >= 0x21 && c <= 0x7e);
    }

    /*
     * Permit only a trusted origin so credentials cannot be redirected through path/query input.
     * Keep diagnostics independent of the rejected value, which may itself contain a secret.
     */
    private static URI parseEndpoint(String value, boolean allowHttp) {
        if (value == null) {
            return null;
        }
        try {
            URI endpoint = new URI(value);
            boolean supportedScheme =
                    "https".equalsIgnoreCase(endpoint.getScheme())
                            || (allowHttp && "http".equalsIgnoreCase(endpoint.getScheme()));
            if (supportedScheme
                    && endpoint.getHost() != null
                    && !endpoint.getHost().contains("%")
                    && endpoint.getRawUserInfo() == null
                    && endpoint.getRawQuery() == null
                    && endpoint.getRawFragment() == null
                    && !endpoint.getRawAuthority().endsWith(":")
                    && endpoint.getPort() != 0
                    && endpoint.getPort() <= 65535
                    && (endpoint.getRawPath().isEmpty() || "/".equals(endpoint.getRawPath()))) {
                return endpoint;
            }
        } catch (URISyntaxException ignored) {
            // An invalid endpoint may contain credential data.
        }
        throw invalid(
                "endpoint_url must be a trusted origin without path, user info, query or fragment");
    }

    private static int requireRange(int value, int min, int max, String option) {
        if (value < min || value > max) {
            throw invalid(option + " must be in [" + min + ", " + max + "]");
        }
        return value;
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Amazon Data Firehose Sink " + reason);
    }
}
