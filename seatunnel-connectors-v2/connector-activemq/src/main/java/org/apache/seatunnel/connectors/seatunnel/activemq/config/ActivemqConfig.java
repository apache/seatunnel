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
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Setter
@Getter
@AllArgsConstructor
public class ActivemqConfig implements Serializable {

    private String username;
    private String password;
    private String uri;
    private String queueName;
    private Boolean checkForDuplicate;
    private String clientID;
    private Integer closeTimeout;
    private Boolean consumerExpiryCheckEnabled;
    private Boolean copyMessageOnSend;
    private Boolean disableTimeStampsByDefault;
    private Boolean dispatchAsync;
    private Boolean nestedMapAndListEnabled;
    private Boolean useCompression;
    private Boolean alwaysSessionAsync;
    private Boolean alwaysSyncSend;
    private Integer warnAboutUnstartedConnectionTimeout;
    private boolean usesCorrelationId = false;
    private Map<String, Object> schema;
    private String format;
    private String fieldDelimiter;

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

    public static final Option<Boolean> COPY_MESSAGE_ON_SEND =
            Options.key("copy_message_on_send")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Should a JMS message be copied to a new JMS Message object as part of the send() method in JMS. "
                                    + "This is enabled by default to be compliant with the JMS specification. "
                                    + "For a performance boost set to false if you do not mutate JMS messages after they are sent.");

    public static final Option<Boolean> DISABLE_TIMESTAMP_BY_DEFAULT =
            Options.key("disable_timeStamps_by_default")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Sets whether or not timestamps on messages should be disabled or not. "
                                    + "For a small performance boost set to false.");

    public static final Option<Boolean> USE_COMPRESSION =
            Options.key("use_compression")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Enables the use of compression on the message’s body.");

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
                    .defaultValue(",")
                    .withDescription("Customize the field delimiter for data format.");

    public static ActivemqConfig of(Config pluginConfig) {
        return of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    public static ActivemqConfig of(ReadonlyConfig config) {

        ActivemqConfig activemqConfig = new ActivemqConfig();

        // common option
        activemqConfig.setUsername(config.get(USERNAME));
        activemqConfig.setPassword(config.get(PASSWORD));
        activemqConfig.setQueueName(config.get(QUEUE_NAME));
        activemqConfig.setUri(config.get(URI));
        activemqConfig.setCheckForDuplicate(config.get(CHECK_FOR_DUPLICATE));
        activemqConfig.setClientID(config.get(CLIENT_ID));
        activemqConfig.setCloseTimeout(config.get(CLOSE_TIMEOUT));
        activemqConfig.setDisableTimeStampsByDefault(config.get(DISABLE_TIMESTAMP_BY_DEFAULT));
        activemqConfig.setWarnAboutUnstartedConnectionTimeout(
                config.get(WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT));

        // sink option
        activemqConfig.setAlwaysSessionAsync(config.get(ALWAYS_SESSION_ASYNC));
        activemqConfig.setAlwaysSyncSend(config.get(ALWAYS_SYNC_SEND));
        activemqConfig.setConsumerExpiryCheckEnabled(config.get(CONSUMER_EXPIRY_CHECK_ENABLED));
        activemqConfig.setCopyMessageOnSend(config.get(COPY_MESSAGE_ON_SEND));
        activemqConfig.setNestedMapAndListEnabled(config.get(NESTED_MAP_AND_LIST_ENABLED));
        activemqConfig.setUseCompression(config.get(USE_COMPRESSION));

        // source option
        activemqConfig.setUsesCorrelationId(config.get(USE_CORRELATION_ID));
        activemqConfig.setSchema(config.get(SCHEMA));
        activemqConfig.setFormat(config.get(FORMAT));
        activemqConfig.setFieldDelimiter(config.get(FIELD_DELIMITER));
        activemqConfig.setDispatchAsync(config.get(DISPATCH_ASYNC));

        return activemqConfig;
    }

    @VisibleForTesting
    public ActivemqConfig() {}
}
