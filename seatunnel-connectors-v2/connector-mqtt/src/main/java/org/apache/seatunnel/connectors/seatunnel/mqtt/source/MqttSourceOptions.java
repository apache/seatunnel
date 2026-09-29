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

package org.apache.seatunnel.connectors.seatunnel.mqtt.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class MqttSourceOptions {

    public static final String CONNECTOR_IDENTITY = "MQTT";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT broker URL, e.g. tcp://localhost:1883");

    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT topic to subscribe messages from");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT broker authentication username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT broker authentication password");

    public static final Option<Integer> QOS =
            Options.key("qos")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "MQTT subscribe QoS level. This controls broker-client delivery only");

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("Message deserialization format: json or text");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field delimiter for text format. Only used when format=text");

    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "MQTT client id. Required when clean_session=false; generated otherwise if omitted");

    public static final Option<Boolean> CLEAN_SESSION =
            Options.key("clean_session")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to use MQTT clean session");

    public static final Option<Integer> CONNECTION_TIMEOUT =
            Options.key("connection_timeout")
                    .intType()
                    .defaultValue(30)
                    .withDescription("MQTT connection timeout in seconds");

    public static final Option<Integer> KEEP_ALIVE_INTERVAL =
            Options.key("keep_alive_interval")
                    .intType()
                    .defaultValue(60)
                    .withDescription("MQTT keep alive interval in seconds");

    public static final Option<Integer> RECONNECT_TIMEOUT =
            Options.key("reconnect_timeout")
                    .intType()
                    .defaultValue(120)
                    .withDescription(
                            "Maximum seconds to wait for MQTT auto-reconnect before failing the source");

    public static final Option<Integer> MAX_QUEUE_SIZE =
            Options.key("max_queue_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum number of MQTT messages buffered before deserialization");
}
