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

package org.apache.seatunnel.edge.agent.transport.config;

import org.apache.seatunnel.api.configuration.util.OptionRule;

public class EdgeTransportOptionRules {

    public static OptionRule rule() {
        return OptionRule.builder()
                .required(
                        EdgeOutputOptions.TYPE,
                        EdgeTransportOptions.ENDPOINT,
                        EdgeTransportOptions.TOKEN)
                .optional(
                        EdgeTransportOptions.AUTH_TYPE,
                        EdgeTransportOptions.PACKET_MODE,
                        EdgeTransportOptions.CONNECT_TIMEOUT_MS,
                        EdgeTransportOptions.READ_TIMEOUT_MS,
                        EdgeTransportOptions.MAX_BATCH_SEND_ATTEMPTS,
                        EdgeTransportOptions.INITIAL_BACKOFF_MS,
                        EdgeTransportOptions.MAX_BACKOFF_MS,
                        EdgeTransportOptions.MAX_RECONNECT_CYCLES)
                .conditionalRule(
                        EdgeTransportOptions.PACKET_MODE,
                        "PACKET",
                        OptionRule.builder()
                                .optional(
                                        EdgeTransportOptions.COMPRESSION,
                                        EdgeTransportOptions.ENCRYPTION)
                                .conditional(
                                        EdgeTransportOptions.ENCRYPTION,
                                        "aes_gcm",
                                        EdgeTransportOptions.AES_SECRET_KEY_BASE64)
                                .build())
                .build();
    }
}
