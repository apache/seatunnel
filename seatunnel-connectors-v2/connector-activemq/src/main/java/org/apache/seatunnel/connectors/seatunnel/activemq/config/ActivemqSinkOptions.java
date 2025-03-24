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

import java.io.Serializable;

public class ActivemqSinkOptions implements Serializable {

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the AMQP user name to use when connecting to the broker");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the password to use when connecting to the broker");

    public static final Option<String> QUEUE_NAME =
            Options.key("queue_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the queue to write the message to");

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host");

    public static final Option<Boolean> CHECK_FOR_DUPLICATE =
            Options.key("check_for_duplicate")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "When true the consumer will check for duplicate messages and properly handle +"
                                    + "the message to make sure that it is not processed twice inadvertently.");
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sets the JMS clientID to use for the connection.");

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

    public static final Option<Integer> CLOSE_TIMEOUT =
            Options.key("close_timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Sets the timeout, in milliseconds, before a close is considered complete. "
                                    + "Normally a close() on a connection waits for confirmation from the broker. "
                                    + "This allows the close operation to timeout preventing the client from hanging when no broker is available.");

    public static final Option<Boolean> DISPATCH_ASYNC =
            Options.key("dispatch_async")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Should the broker dispatch messages asynchronously to the consumer?");

    public static final Option<Boolean> NESTED_MAP_AND_LIST_ENABLED =
            Options.key("nested_map_and_list_enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls whether Structured Message Properties and MapMessages are supported "
                                    + "so that Message properties and MapMessage entries can contain nested Map and List objects."
                                    + " Available from version 4.1.");

    public static final Option<Integer> WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT =
            Options.key("warn_about_unstarted_connection_timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The timeout, in milliseconds, from the time of connection creation to when a warning is generated "
                                    + "if the connection is not properly started via Connection.start() and a message is received by a consumer. "
                                    + "It is a very common gotcha to forget to start the connection and then wonder why no messages are delivered "
                                    + "so this option makes the default case to create a warning if the user forgets. "
                                    + "To disable the warning just set the value to < 0.");

    public static final Option<Boolean> CONSUMER_EXPIRY_CHECK_ENABLED =
            Options.key("consumer_expiry_check_enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls whether message expiration checking is done in each "
                                    + "MessageConsumer prior to dispatching a message.");
}
