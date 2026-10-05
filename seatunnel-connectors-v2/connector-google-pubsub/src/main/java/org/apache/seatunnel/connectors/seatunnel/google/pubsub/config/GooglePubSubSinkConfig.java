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

@Getter
@Builder
public class GooglePubSubSinkConfig implements Serializable {

    private final String projectId;
    private final String topic;
    private final String credentialsPath;
    private final String emulatorHost;
    private final MessageFormat format;
    private final String fieldDelimiter;

    public static GooglePubSubSinkConfig from(ReadonlyConfig config) {
        String projectId = config.get(GooglePubSubSinkOptions.PROJECT_ID);
        String topic = config.get(GooglePubSubSinkOptions.TOPIC);
        String credentialsPath = config.get(GooglePubSubSinkOptions.CREDENTIALS_PATH);
        String emulatorHost = config.get(GooglePubSubSinkOptions.EMULATOR_HOST);
        MessageFormat format = config.get(GooglePubSubSinkOptions.FORMAT);
        String fieldDelimiter = config.get(GooglePubSubSinkOptions.FIELD_DELIMITER);

        requireNonBlank(projectId, GooglePubSubSinkOptions.PROJECT_ID.key());
        requireNonBlank(topic, GooglePubSubSinkOptions.TOPIC.key());
        requireNonBlankIfPresent(credentialsPath, GooglePubSubSinkOptions.CREDENTIALS_PATH.key());
        requireNonBlankIfPresent(emulatorHost, GooglePubSubSinkOptions.EMULATOR_HOST.key());
        if (credentialsPath != null && emulatorHost != null) {
            throw new IllegalArgumentException(
                    "Options 'credentials_path' and 'emulator_host' cannot be configured together");
        }
        if (format == MessageFormat.TEXT && fieldDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Option 'field_delimiter' cannot be empty");
        }

        return GooglePubSubSinkConfig.builder()
                .projectId(projectId)
                .topic(topic)
                .credentialsPath(credentialsPath)
                .emulatorHost(emulatorHost)
                .format(format)
                .fieldDelimiter(fieldDelimiter)
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
}
