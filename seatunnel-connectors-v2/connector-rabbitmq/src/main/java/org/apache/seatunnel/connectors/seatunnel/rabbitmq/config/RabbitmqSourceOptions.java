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

public class RabbitmqSourceOptions extends RabbitmqBaseOptions {

    public static final Option<Integer> REQUESTED_CHANNEL_MAX =
            Options.key("requested_channel_max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("initially requested maximum channel number");

    public static final Option<Integer> REQUESTED_FRAME_MAX =
            Options.key("requested_frame_max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the requested maximum frame size");

    public static final Option<Integer> REQUESTED_HEARTBEAT =
            Options.key("requested_heartbeat")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the requested heartbeat timeout");

    public static final Option<Integer> PREFETCH_COUNT =
            Options.key("prefetch_count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "prefetchCount the max number of messages to receive without acknowledgement\n");

    public static final Option<Integer> DELIVERY_TIMEOUT =
            Options.key("delivery_timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription("deliveryTimeout maximum wait time");

    public static final Option<Boolean> USE_CORRELATION_ID =
            Options.key("use_correlation_id")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the messages received are supplied with a unique"
                                    + "id to deduplicate messages (in case of failed acknowledgments).");
}
