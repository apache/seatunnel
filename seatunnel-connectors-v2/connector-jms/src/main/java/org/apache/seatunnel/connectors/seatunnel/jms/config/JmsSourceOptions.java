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

package org.apache.seatunnel.connectors.seatunnel.jms.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options of the JMS queue source. */
public class JmsSourceOptions extends JmsBaseOptions {

    public enum MessageFormat {
        JSON,
        TEXT
    }

    public static final Option<String> MESSAGE_SELECTOR =
            Options.key("message_selector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JMS message selector applied by the broker.");

    public static final Option<MessageFormat> FORMAT =
            Options.key("format")
                    .enumType(MessageFormat.class)
                    .defaultValue(MessageFormat.JSON)
                    .withDescription(
                            "Payload format of TextMessage and BytesMessage bodies: JSON or "
                                    + "delimited TEXT.");

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
                            "Maximum number of messages a reader has received but not yet "
                                    + "committed. At the limit the reader stops receiving until "
                                    + "a checkpoint completes. Keep it below any broker limit on "
                                    + "uncommitted messages per transaction.");
}
