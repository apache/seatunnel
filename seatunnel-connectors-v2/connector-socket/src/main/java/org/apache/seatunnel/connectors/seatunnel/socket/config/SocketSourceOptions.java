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

public class SocketSourceOptions extends SocketCommonOptions {

    public static final int DEFAULT_READ_TIMEOUT_MS = 0;

    /**
     * SO_TIMEOUT for the source's blocking read. Defaults to 0, which keeps the historical blocking
     * behaviour that long-lived socket sources rely on. This option is source-only: the sink never
     * reads from its socket, so a read timeout would be a no-op there.
     */
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_READ_TIMEOUT_MS)
                    .withDescription(
                            "Socket read (SO_TIMEOUT) in milliseconds for the source. A value of 0 "
                                    + "(default) means no application-level timeout is applied, so "
                                    + "the read blocks until data arrives or the socket closes; "
                                    + "the OS default then governs the connection.");
}
