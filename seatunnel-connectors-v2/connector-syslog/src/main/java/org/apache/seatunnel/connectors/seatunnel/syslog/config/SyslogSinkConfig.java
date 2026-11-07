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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.connectors.seatunnel.syslog.sink.SyslogSinkFactory;

import lombok.Getter;

import java.io.Serializable;

/** Serializable configuration only; TLS resources are opened on the worker. */
@Getter
public final class SyslogSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final int connectTimeout;
    private final int writeTimeout;
    private final int maxMessageBytes;
    private final String caCert;
    private final String keyStore;
    private final String keyStorePassword;
    private final String keyStoreType;

    public SyslogSinkConfig(ReadonlyConfig config) {
        ConfigValidator.of(config).validate(new SyslogSinkFactory().optionRule());
        host = config.get(SyslogSinkOptions.HOST);
        if (!host.equals(host.trim()) || host.chars().anyMatch(c -> c <= 32 || c >= 127)) {
            throw new IllegalArgumentException(
                    "Syslog host must be an ASCII hostname or IP address without whitespace");
        }
        port = config.get(SyslogSinkOptions.PORT);
        connectTimeout = config.get(SyslogSinkOptions.CONNECT_TIMEOUT);
        writeTimeout = config.get(SyslogSinkOptions.WRITE_TIMEOUT);
        maxMessageBytes = config.get(SyslogSinkOptions.MAX_MESSAGE_BYTES);
        caCert = config.getOptional(SyslogSinkOptions.CA_CERT).orElse(null);
        keyStore = config.getOptional(SyslogSinkOptions.KEY_STORE).orElse(null);
        keyStorePassword = config.getOptional(SyslogSinkOptions.KEY_STORE_PASSWORD).orElse(null);
        keyStoreType = config.get(SyslogSinkOptions.KEY_STORE_TYPE);
        if (!"PKCS12".equals(keyStoreType) && !"JKS".equals(keyStoreType)) {
            throw new IllegalArgumentException("Syslog tls.key_store.type must be PKCS12 or JKS");
        }
        if ((keyStore == null) != (keyStorePassword == null)) {
            throw new IllegalArgumentException(
                    "Syslog tls.key_store.path and password must be configured together");
        }
    }
}
