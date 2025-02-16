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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@AllArgsConstructor
public class RabbitmqBaseOptions implements Serializable {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host");

    public static final Option<Integer> NETWORK_RECOVERY_INTERVAL =
            Options.key("network_recovery_interval")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "how long will automatic recovery wait before attempting to reconnect, in ms");

    public static final Option<Boolean> AUTOMATIC_RECOVERY_ENABLED =
            Options.key("AUTOMATIC_RECOVERY_ENABLED")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("if true, enables connection recovery");

    public static final Option<Boolean> TOPOLOGY_RECOVERY_ENABLED =
            Options.key("topology_recovery_enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("if true, enables topology recovery");

    public static final Option<Integer> CONNECTION_TIMEOUT =
            Options.key("connection_timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription("connection TCP establishment timeout in milliseconds");

    public static final Option<String> ROUTING_KEY =
            Options.key("routing_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the routing key to publish the message to");

    public static final Option<String> EXCHANGE =
            Options.key("exchange")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the exchange to publish the message to");
}
