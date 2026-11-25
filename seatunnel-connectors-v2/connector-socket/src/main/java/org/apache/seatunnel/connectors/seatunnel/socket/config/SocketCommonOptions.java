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

package org.apache.seatunnel.connectors.seatunnel.socket.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SocketCommonOptions {

    public static final String identifier = "Socket";

    public static final Option<String> HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("socket host");

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("socket port");

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 10000;

    /**
     * Bound on the TCP connect for both source and sink. Without it an unreachable or filtered peer
     * blocks the connector thread until the OS-level TCP timeout, which is minutes. Note that 0
     * means "no application-level timeout", so the OS default applies.
     */
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_CONNECT_TIMEOUT_MS)
                    .withDescription(
                            "Socket connect timeout in milliseconds. A value of 0 disables the "
                                    + "application-level timeout, leaving the OS default. Default is "
                                    + DEFAULT_CONNECT_TIMEOUT_MS
                                    + " ms");
}
