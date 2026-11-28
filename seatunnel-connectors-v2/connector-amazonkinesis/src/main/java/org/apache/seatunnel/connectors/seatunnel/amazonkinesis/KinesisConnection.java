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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * One SDK/transport owner. SDK retries are disabled to keep the producer acknowledgment boundary.
 */
final class KinesisConnection implements AutoCloseable {
    private final KinesisTransport transport;
    private final DefaultCredentialsProvider defaultProvider;
    private final AtomicBoolean closed = new AtomicBoolean();
    final KinesisClient sdk;

    KinesisConnection(Settings settings) {
        transport = new KinesisTransport(settings.region, settings.timeoutMs);
        DefaultCredentialsProvider provider = null;
        try {
            KinesisClientBuilder builder =
                    KinesisClient.builder()
                            .region(Region.of(settings.region))
                            .httpClient(transport)
                            .putAuthScheme(new KinesisSigning())
                            .overrideConfiguration(
                                    options ->
                                            options.retryPolicy(RetryPolicy.none())
                                                    .apiCallAttemptTimeout(
                                                            Duration.ofMillis(settings.timeoutMs))
                                                    .apiCallTimeout(
                                                            Duration.ofMillis(settings.timeoutMs)));
            if (settings.accessKey == null) {
                provider = DefaultCredentialsProvider.builder().build();
                builder.credentialsProvider(provider);
            } else {
                builder.credentialsProvider(
                        StaticCredentialsProvider.create(
                                settings.token == null
                                        ? AwsBasicCredentials.create(
                                                settings.accessKey, settings.secretKey)
                                        : AwsSessionCredentials.create(
                                                settings.accessKey,
                                                settings.secretKey,
                                                settings.token)));
            }
            sdk = builder.build();
            defaultProvider = provider;
        } catch (RuntimeException ex) {
            transport.close();
            if (provider != null) {
                provider.close();
            }
            throw new IllegalStateException("Could not initialize Kinesis SDK client");
        }
    }

    /** Retries only read operations. Cursor expiration is handled by the logical reader. */
    <T> T read(String operation, Supplier<T> action) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            boolean throttled = false;
            if (closed.get() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Kinesis source closed");
            }
            try {
                return action.get();
            } catch (KinesisException ex) {
                String code = ex.awsErrorDetails() == null ? "" : ex.awsErrorDetails().errorCode();
                if ("ExpiredIteratorException".equals(code)
                        || "ExpiredNextTokenException".equals(code)) {
                    throw new ReadFailure(operation, code);
                }
                throttled =
                        ex.statusCode() == 429
                                || "ProvisionedThroughputExceededException".equals(code)
                                || "LimitExceededException".equals(code);
                if (attempt >= 3 || (!throttled && ex.statusCode() < 500)) {
                    String error =
                            "AccessDeniedException".equals(code)
                                    ? "access denied"
                                    : "ResourceNotFoundException".equals(code)
                                            ? "stream missing"
                                            : "InvalidArgumentException".equals(code)
                                                    ? "invalid request or expired retained position"
                                                    : "service request failed";
                    throw new ReadFailure(operation, error);
                }
            } catch (SdkClientException ex) {
                if (attempt >= 3) {
                    throw new ReadFailure(operation, "transport or response failure");
                }
            }
            if (throttled) {
                TimeUnit.MILLISECONDS.sleep(1000 + ThreadLocalRandom.current().nextLong(251));
            } else {
                waitRetry(attempt);
            }
        }
    }

    static void waitRetry(int attempt) throws InterruptedException {
        long wait = Math.min(1000, 120L << (attempt - 1));
        TimeUnit.MILLISECONDS.sleep(wait + ThreadLocalRandom.current().nextLong(wait + 1));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            transport.close();
            try {
                sdk.close();
            } finally {
                if (defaultProvider != null) {
                    defaultProvider.close();
                }
            }
        }
    }

    static final class ReadFailure extends IllegalStateException {
        private final String reason;

        ReadFailure(String operation, String reason) {
            super("Kinesis " + operation + ": " + reason);
            this.reason = reason;
        }

        boolean expiredIterator() {
            return "ExpiredIteratorException".equals(reason);
        }

        boolean expiredToken() {
            return "ExpiredNextTokenException".equals(reason);
        }
    }

    static final class Settings implements Serializable {
        private static final long serialVersionUID = 1L;
        static final Option<String> REGION =
                Options.key("region")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("AWS signing region");
        static final Option<String> STREAM_NAME =
                Options.key("stream_name")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                "Kinesis Data Streams name, mutually exclusive with stream_arn");
        static final Option<String> STREAM_ARN =
                Options.key("stream_arn")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                "Kinesis Data Streams ARN, mutually exclusive with stream_name");
        static final Option<String> ACCESS_KEY =
                Options.key("access_key")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                "AWS access key ID. Omit both keys to use the default credential provider chain");
        static final Option<String> SECRET_KEY =
                Options.key("secret_key")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("AWS secret access key");
        static final Option<String> TOKEN =
                Options.key("token")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("AWS session token for explicit temporary credentials");
        static final Option<Integer> API_TIMEOUT_MS =
                Options.key("api_timeout_ms")
                        .intType()
                        .defaultValue(10000)
                        .withDescription(
                                "Whole-call and socket timeout in milliseconds, 500 to 60000");

        final String region;
        final String streamName;
        final String streamArn;
        final int timeoutMs;
        private final String accessKey;
        private final String secretKey;
        private final String token;

        Settings(ReadonlyConfig options) {
            region = options.get(REGION);
            streamArn = options.get(STREAM_ARN);
            streamName = options.get(STREAM_NAME);
            if (region == null
                    || !region.matches("[a-z0-9]+(?:-[a-z0-9]+)+")
                    || (streamArn == null) == (streamName == null)) {
                throw new IllegalArgumentException(
                        "Kinesis requires a valid region and exactly one stream identifier");
            }
            if (streamArn != null
                            && !streamArn.matches(
                                    "arn:aws[a-z0-9-]*:kinesis:"
                                            + region
                                            + ":[0-9]{12}:stream/[A-Za-z0-9_.-]{1,128}")
                    || streamName != null && !streamName.matches("[A-Za-z0-9_.-]{1,128}")) {
                throw new IllegalArgumentException(
                        "Kinesis stream identity is invalid for the selected region");
            }
            accessKey = credential(options, ACCESS_KEY);
            secretKey = credential(options, SECRET_KEY);
            token = credential(options, TOKEN);
            if ((accessKey == null) != (secretKey == null)
                    || (token != null && accessKey == null)) {
                throw new IllegalArgumentException(
                        "Kinesis explicit credentials require access_key and secret_key");
            }
            timeoutMs = bounded(options, API_TIMEOUT_MS, 500, 60000);
        }

        String scope() {
            return region + "/" + (streamArn == null ? streamName : streamArn);
        }

        private String credential(ReadonlyConfig options, Option<String> option) {
            String value = options.get(option);
            if (value != null
                    && (value.isEmpty()
                            || value.length() > 8192
                            || !value.chars().allMatch(c -> c >= 33 && c <= 126))) {
                throw new IllegalArgumentException("Invalid Kinesis option " + option.key());
            }
            return value;
        }

        static int bounded(ReadonlyConfig options, Option<Integer> option, int min, int max) {
            int value = options.get(option);
            if (value < min || value > max) {
                throw new IllegalArgumentException(
                        "Kinesis option is out of range: " + option.key());
            }
            return value;
        }
    }
}
