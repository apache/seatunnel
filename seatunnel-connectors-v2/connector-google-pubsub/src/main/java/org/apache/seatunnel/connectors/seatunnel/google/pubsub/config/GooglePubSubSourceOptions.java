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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

public class GooglePubSubSourceOptions extends ConnectorCommonOptions {

    public static final Option<String> PROJECT_ID =
            Options.key("project_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google Cloud project ID.");

    public static final Option<String> SUBSCRIPTION =
            Options.key("subscription")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google Pub/Sub subscription ID.");

    public static final Option<String> CREDENTIALS_PATH =
            Options.key("credentials_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to a Google Cloud service account JSON key file. "
                                    + "Application Default Credentials are used when this option is not set.");

    public static final Option<String> EMULATOR_HOST =
            Options.key("emulator_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Pub/Sub emulator host and port, for example pubsub-emulator:8085. "
                                    + "Authentication and TLS are disabled when this option is set.");

    public static final Option<MessageFormat> FORMAT =
            Options.key("format")
                    .enumType(MessageFormat.class)
                    .defaultValue(MessageFormat.JSON)
                    .withDescription("Message payload format. Supported values are json and text.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field delimiter used when format is text.");

    public static final Option<Long> MAX_OUTSTANDING_MESSAGES =
            Options.key("max_outstanding_messages")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of messages held by the Pub/Sub subscriber. "
                                    + "The Google client default is used when this option is not set.");

    public static final Option<Long> MAX_OUTSTANDING_BYTES =
            Options.key("max_outstanding_bytes")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum total bytes held by the Pub/Sub subscriber. "
                                    + "The Google client default is used when this option is not set.");

    public static final Option<Integer> PARALLEL_PULL_COUNT =
            Options.key("parallel_pull_count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Number of streaming pull connections used by the Pub/Sub subscriber. "
                                    + "The Google client default is used when this option is not set.");

    private GooglePubSubSourceOptions() {}
}
