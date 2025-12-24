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

package org.apache.seatunnel.connectors.seatunnel.kafka.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.util.Map;

public class KafkaBaseOptions extends ConnectorCommonOptions {

    public static final String CONNECTOR_IDENTITY = "Kafka";
    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final Option<Map<String, String>> KAFKA_CONFIG =
            Options.key("kafka.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "In addition to the above parameters that must be specified by the Kafka producer or consumer client, "
                                    + "the user can also specify multiple non-mandatory parameters for the producer or consumer client, "
                                    + "covering all the producer parameters specified in the official Kafka document.");

    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kafka topic name. If there are multiple topics, use , to split, for example: \"tpc1,tpc2\".");

    public static final Option<String> BOOTSTRAP_SERVERS =
            Options.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka cluster address, separated by \",\".");

    public static final Option<MessageFormat> FORMAT =
            Options.key("format")
                    .enumType(MessageFormat.class)
                    .defaultValue(MessageFormat.JSON)
                    .withDescription(
                            "Data format. The default format is json. Optional text format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field_delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription("Customize the field delimiter for data format.");

    public static final Option<String> PROTOBUF_SCHEMA =
            Options.key("protobuf_schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Data serialization method protobuf metadata, used to parse protobuf data.");

    public static final Option<String> PROTOBUF_MESSAGE_NAME =
            Options.key("protobuf_message_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Parsing entity class names from Protobuf data.");
}
