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

package org.apache.seatunnel.connectors.seatunnel.emqx.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class Config {

    public static final String CONNECTOR_IDENTITY = "Emqx";

    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Subscribe to one or more topics. Use a comma (,) to separate each topic when subscribing to multiple topics. e.g. test1,test2");

    public static final Option<String> BROKER =
            Options.key("broker")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Emqx broker address, for example: \"tcp://broker.emqx.io:1883\".");

    public static final Option<String> CLIENT_ID =
            Options.key("clientId")
                    .stringType()
                    .defaultValue("SeaTunnel-Client")
                    .withDescription(
                            "The client ID is used to identify the client in the broker. ");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username used to connect to the broker.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password used to connect to the broker.");

    public static final Option<Boolean> CLEAN_SESSION =
            Options.key("cleanSession")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Clean session. The default value is true. "
                                    + "If the clean session is set to true, the broker will clean up the client's session data when the client disconnects.");

    public static final Option<PayloadFormat> FORMAT =
            Options.key("format")
                    .enumType(PayloadFormat.class)
                    .defaultValue(PayloadFormat.TEXT)
                    .withDescription(
                            "Payload format. The default format is text. Optional json format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field_delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription("Customize the field delimiter for data format.");

    public static final Option<MessageFormatErrorHandleWay> MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION =
            Options.key("format_error_handle_way")
                    .enumType(MessageFormatErrorHandleWay.class)
                    .defaultValue(MessageFormatErrorHandleWay.FAIL)
                    .withDescription(
                            "The processing method of data format error. The default value is fail, and the optional value is (fail, skip). "
                                    + "When fail is selected, data format error will block and an exception will be thrown. "
                                    + "When skip is selected, data format error will skip this line data.");

    public static final Option<Integer> QOS =
            Options.key("QoS")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The QoS level of the message. The default value is 0. Optional values are (0, 1, 2). "
                                    + "0: At most once, 1: At least once, 2: Exactly once.");
}
