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

public class ActivemqSinkOptions extends ActivemqOptions {

    public static final Option<Boolean> ALWAYS_SESSION_ASYNC =
            Options.key("always_session_async")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "When true a separate thread is used for dispatching messages for each Session in the Connection. "
                                    + "A separate thread is always used when there’s more than one session, "
                                    + "or the session isn’t in Session.AUTO_ACKNOWLEDGE or Session.DUPS_OK_ACKNOWLEDGE mode.");

    public static final Option<Boolean> ALWAYS_SYNC_SEND =
            Options.key("always_sync_send")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "When true a MessageProducer will always use Sync sends when sending a Message "
                                    + "even if it is not required for the Delivery Mode.");

    public static final Option<Boolean> COPY_MESSAGE_ON_SEND =
            Options.key("copy_message_on_send")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Should a JMS message be copied to a new JMS Message object as part of the send() method in JMS. "
                                    + "This is enabled by default to be compliant with the JMS specification. "
                                    + "For a performance boost set to false if you do not mutate JMS messages after they are sent.");

    public static final Option<Boolean> NESTED_MAP_AND_LIST_ENABLED =
            Options.key("nested_map_and_list_enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls whether Structured Message Properties and MapMessages are supported "
                                    + "so that Message properties and MapMessage entries can contain nested Map and List objects."
                                    + " Available from version 4.1.");

    public static final Option<Boolean> USE_COMPRESSION =
            Options.key("use_compression")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Enables the use of compression on the message’s body.");
}
