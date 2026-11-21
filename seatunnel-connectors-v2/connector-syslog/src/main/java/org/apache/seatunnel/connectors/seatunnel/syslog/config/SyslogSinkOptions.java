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

package org.apache.seatunnel.connectors.seatunnel.syslog.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class SyslogSinkOptions {
    public static final String IDENTIFIER = "Syslog";
    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TLS receiver hostname, verified against its certificate.");
    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .defaultValue(6514)
                    .withDescription("TLS receiver port (1-65535).");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "TCP connect timeout in milliseconds, excluding JVM DNS resolution.");
    public static final Option<Integer> WRITE_TIMEOUT =
            Options.key("write_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Deadline in milliseconds for a TLS handshake, write/flush, or close.");
    public static final Option<Integer> MAX_MESSAGE_BYTES =
            Options.key("max_message_bytes")
                    .intType()
                    .defaultValue(8192)
                    .withDescription(
                            "Maximum UTF-8 syslog message bytes, including header and BOM, excluding framing (1-1048576).");
    public static final Option<String> CA_CERT =
            Options.key("tls.ca_cert_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM CA bundle. When absent, use the JVM default trust anchors.");
    public static final Option<String> KEY_STORE =
            Options.key("tls.key_store.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional worker-local client key store for mutual TLS.");
    public static final Option<String> KEY_STORE_PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for the client key store and private key.");
    public static final Option<String> KEY_STORE_TYPE =
            Options.key("tls.key_store.type")
                    .stringType()
                    .defaultValue("PKCS12")
                    .withDescription("Client key store type: PKCS12 or JKS.");

    private SyslogSinkOptions() {}
}
