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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options for consuming one ActiveMQ Classic queue. */
public class ActivemqSourceOptions extends ActivemqBaseOptions {
    public enum MessageFormat {
        JSON,
        TEXT
    }

    public static final Option<MessageFormat> FORMAT =
            Options.key("format")
                    .enumType(MessageFormat.class)
                    .defaultValue(MessageFormat.JSON)
                    .withDescription("TextMessage payload format: JSON or delimited TEXT.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field delimiter for TEXT payloads.");

    public static final Option<Integer> MAX_IN_FLIGHT_MESSAGES =
            Options.key("max_in_flight_messages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum emitted messages retained per reader until checkpoint completion. "
                                    + "Also bounds the client queue prefetch; this is a count, not a byte limit.");

    public static final Option<String> MESSAGE_SELECTOR =
            Options.key("message_selector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JMS selector expression for broker-side filtering. "
                                    + "Only messages matching the selector are delivered to the consumer.");

    public static final Option<Integer> RECEIVE_TIMEOUT =
            Options.key("receive_timeout")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "Timeout in milliseconds for consumer.receive(). "
                                    + "Smaller values improve checkpoint responsiveness "
                                    + "but increase CPU usage when the queue is idle. "
                                    + "Defaults to 200.");
}
