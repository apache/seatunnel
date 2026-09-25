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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

public class AzureQueueStorageSinkOptions extends ConnectorCommonOptions {

    public static final Option<String> QUEUE_NAME =
            Options.key("queue_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the target Azure Storage queue.");

    public static final Option<AuthenticationType> AUTHENTICATION_TYPE =
            Options.key("authentication_type")
                    .enumType(AuthenticationType.class)
                    .noDefaultValue()
                    .withDescription(
                            "Authentication mode. Supported values are connection_string, shared_key and sas_token.");

    public static final Option<String> CONNECTION_STRING =
            Options.key("connection_string")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Azure Storage connection string. Required when authentication_type is connection_string.");

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Azure Queue service endpoint. Required for shared_key and sas_token authentication.");

    public static final Option<String> ACCOUNT_NAME =
            Options.key("account_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Azure Storage account name. Required when authentication_type is shared_key.");

    public static final Option<String> ACCOUNT_KEY =
            Options.key("account_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Azure Storage account key. Required when authentication_type is shared_key.");

    public static final Option<String> SAS_TOKEN =
            Options.key("sas_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Azure Storage SAS token. Required when authentication_type is sas_token.");

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

    public static final Option<MessageEncoding> MESSAGE_ENCODING =
            Options.key("message_encoding")
                    .enumType(MessageEncoding.class)
                    .defaultValue(MessageEncoding.NONE)
                    .withDescription(
                            "Azure Queue message encoding. Supported values are none and base64.");

    public static final Option<Integer> MAX_IN_FLIGHT =
            Options.key("max_in_flight")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum number of queue sends that may be outstanding for each sink task.");

    public static final Option<Long> OPERATION_TIMEOUT_MS =
            Options.key("operation_timeout_ms")
                    .longType()
                    .defaultValue(60_000L)
                    .withDescription(
                            "Maximum time in milliseconds to wait for send capacity or checkpoint-time flushing.");

    private AzureQueueStorageSinkOptions() {}
}
