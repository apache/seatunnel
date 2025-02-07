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

package org.apache.seatunnel.connectors.seatunnel.activemq.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class ActivemqSourceOptions extends ActivemqOptions {

    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final Option<Boolean> DISPATCH_ASYNC =
            Options.key("dispatch_async")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Should the broker dispatch messages asynchronously to the consumer?");

    public static final Option<Boolean> USE_CORRELATION_ID =
            Options.key("use_correlation_id")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the messages received are supplied with a unique"
                                    + "id to deduplicate messages (in case of failed acknowledgments).");

    public static final Option<Map<String, Object>> SCHEMA =
            Options.key("schema")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "The structure of the data, including field names and field types.");

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue(SchemaFormat.JSON.getName())
                    .withDescription(
                            "Data format. The default format is json. Optional text format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field.delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field.delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription("Customize the field delimiter for data format.");

    public static final Option<Boolean> CONSUMER_EXPIRY_CHECK_ENABLED =
            Options.key("consumer_expiry_check_enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls whether message expiration checking is done in each "
                                    + "MessageConsumer prior to dispatching a message.");
}
