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

public class ActivemqSinkOptions extends ActivemqBaseOptions {

    public static final Option<Boolean> ALWAYS_SYNC_SEND =
            Options.key("always_sync_send")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "When true a MessageProducer will always use Sync sends when sending a Message "
                                    + "even if it is not required for the Delivery Mode.");

    public static final Option<Boolean> NESTED_MAP_AND_LIST_ENABLED =
            Options.key("nested_map_and_list_enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls whether Structured Message Properties and MapMessages are supported "
                                    + "so that Message properties and MapMessage entries can contain nested Map and List objects."
                                    + " Available from version 4.1.");

    public static final Option<Integer> MAX_THREAD_POOL_SIZE =
            Options.key("max_thread_pool_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Sets the maximum thread pool size for the ActiveMQ connection factory's "
                                    + "internal thread pool used for dispatching messages.");

    public static final Option<Integer> SEND_TIMEOUT =
            Options.key("send_timeout")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Sets the send timeout in milliseconds for the ActiveMQ connection factory. "
                                    + "A value of 0 means no timeout.");

    public static final Option<Boolean> USE_COMPRESSION =
            Options.key("use_compression")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, message bodies are compressed before being sent to the broker.");

    public static final Option<Integer> DELIVERY_MODE =
            Options.key("delivery_mode")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "JMS delivery mode: 1 for NON_PERSISTENT (message kept in memory, "
                                    + "lost if broker crashes) or 2 for PERSISTENT (message written "
                                    + "to disk before acknowledge). Defaults to 2 (PERSISTENT).");

    public static final Option<Integer> PRIORITY =
            Options.key("priority")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "JMS message priority from 0 (lowest) to 9 (highest). The broker "
                                    + "delivers higher-priority messages before lower-priority ones. "
                                    + "Defaults to 4.");

    public static final Option<Integer> PRODUCER_WINDOW_SIZE =
            Options.key("producer_window_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Flow control window size in bytes for async sends. A value of 0 means "
                                    + "no flow control. Used together with use_async_send to prevent "
                                    + "unbounded memory growth. Defaults to 0.");

    public static final Option<Long> TIME_TO_LIVE =
            Options.key("time_to_live")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Message time-to-live in milliseconds. A value of 0 means the message "
                                    + "never expires. The broker discards messages after the TTL "
                                    + "expires. Defaults to 0.");

    public static final Option<Boolean> USE_ASYNC_SEND =
            Options.key("use_async_send")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, the producer sends messages without waiting for the broker "
                                    + "to confirm receipt, improving throughput at the risk of message "
                                    + "loss if the broker fails. Defaults to false.");
}
