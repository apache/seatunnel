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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class ProducerConfig extends Config {

    public static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024 * 4;
    public static final int DEFAULT_SEND_MESSAGE_TIMEOUT_MILLIS = 3000;
    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMq topic name. ");

    public static final Option<String> PRODUCER_GROUP =
            Options.key("producer.group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMq producer group id.");

    public static final Option<List<String>> PARTITION_KEY_FIELDS =
            Options.key("partition.key.fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Configure which fields are used as the key of the RocketMq message.");

    public static final Option<Boolean> EXACTLY_ONCE =
            Options.key("exactly.once")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If true, the transaction message will be sent.");

    public static final Option<Boolean> SEND_SYNC =
            Options.key("producer.send.sync")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If true, the message will be sync sent.");

    public static final Option<Integer> MAX_MESSAGE_SIZE =
            Options.key("max.message.size")
                    .intType()
                    .defaultValue(DEFAULT_MAX_MESSAGE_SIZE)
                    .withDescription("Maximum allowed message body size in bytes.");

    public static final Option<Integer> SEND_MESSAGE_TIMEOUT_MILLIS =
            Options.key("send.message.timeout")
                    .intType()
                    .defaultValue(DEFAULT_SEND_MESSAGE_TIMEOUT_MILLIS)
                    .withDescription("Timeout for sending messages.");
}
