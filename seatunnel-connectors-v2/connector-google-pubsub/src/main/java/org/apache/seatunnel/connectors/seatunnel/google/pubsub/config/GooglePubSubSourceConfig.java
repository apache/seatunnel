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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/** Immutable runtime configuration for the Google Pub/Sub source. */
@Getter
@Builder
public class GooglePubSubSourceConfig implements Serializable {

    private final String projectId;
    private final String subscription;
    private final String credentialsPath;
    private final String emulatorHost;
    private final MessageFormat format;
    private final String fieldDelimiter;
    private final Long maxOutstandingMessages;
    private final Long maxOutstandingBytes;
    private final Integer parallelPullCount;

    public static GooglePubSubSourceConfig from(ReadonlyConfig config) {
        String projectId = config.get(GooglePubSubSourceOptions.PROJECT_ID);
        String subscription = config.get(GooglePubSubSourceOptions.SUBSCRIPTION);
        String credentialsPath = config.get(GooglePubSubSourceOptions.CREDENTIALS_PATH);
        String emulatorHost = config.get(GooglePubSubSourceOptions.EMULATOR_HOST);
        MessageFormat format = config.get(GooglePubSubSourceOptions.FORMAT);
        String fieldDelimiter = config.get(GooglePubSubSourceOptions.FIELD_DELIMITER);
        Long maxOutstandingMessages =
                config.get(GooglePubSubSourceOptions.MAX_OUTSTANDING_MESSAGES);
        Long maxOutstandingBytes = config.get(GooglePubSubSourceOptions.MAX_OUTSTANDING_BYTES);
        Integer parallelPullCount = config.get(GooglePubSubSourceOptions.PARALLEL_PULL_COUNT);

        requireNonBlank(projectId, GooglePubSubSourceOptions.PROJECT_ID.key());
        requireNonBlank(subscription, GooglePubSubSourceOptions.SUBSCRIPTION.key());
        requireNonBlankIfPresent(credentialsPath, GooglePubSubSourceOptions.CREDENTIALS_PATH.key());
        requireNonBlankIfPresent(emulatorHost, GooglePubSubSourceOptions.EMULATOR_HOST.key());
        if (credentialsPath != null && emulatorHost != null) {
            throw new IllegalArgumentException(
                    "Options 'credentials_path' and 'emulator_host' cannot be configured together");
        }
        if (format == MessageFormat.TEXT && fieldDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Option 'field_delimiter' cannot be empty");
        }
        requirePositive(
                maxOutstandingMessages, GooglePubSubSourceOptions.MAX_OUTSTANDING_MESSAGES.key());
        requirePositive(maxOutstandingBytes, GooglePubSubSourceOptions.MAX_OUTSTANDING_BYTES.key());
        requirePositive(parallelPullCount, GooglePubSubSourceOptions.PARALLEL_PULL_COUNT.key());

        return GooglePubSubSourceConfig.builder()
                .projectId(projectId)
                .subscription(subscription)
                .credentialsPath(credentialsPath)
                .emulatorHost(emulatorHost)
                .format(format)
                .fieldDelimiter(fieldDelimiter)
                .maxOutstandingMessages(maxOutstandingMessages)
                .maxOutstandingBytes(maxOutstandingBytes)
                .parallelPullCount(parallelPullCount)
                .build();
    }

    private static void requireNonBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Option '" + option + "' cannot be blank");
        }
    }

    private static void requireNonBlankIfPresent(String value, String option) {
        if (value != null) {
            requireNonBlank(value, option);
        }
    }

    private static void requirePositive(Number value, String option) {
        if (value != null && value.longValue() <= 0) {
            throw new IllegalArgumentException("Option '" + option + "' must be greater than 0");
        }
    }
}
