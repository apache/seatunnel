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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

@Data
public class HttpConfig implements Serializable {

    private boolean enabled =
            ServerConfigOptions.MasterServerConfigOptions.ENABLE_HTTP.defaultValue();

    private int port = ServerConfigOptions.MasterServerConfigOptions.PORT.defaultValue();

    /** Whether to enable https. */
    private boolean enableHttps =
            ServerConfigOptions.MasterServerConfigOptions.ENABLE_HTTPS.defaultValue();

    /** The port of https. */
    private int httpsPort = ServerConfigOptions.MasterServerConfigOptions.HTTPS_PORT.defaultValue();

    /** The path of keystore file. */
    private String keyStorePath =
            ServerConfigOptions.MasterServerConfigOptions.KEY_STORE_PATH.defaultValue();

    /** The password of keystore file. */
    private String keyStorePassword =
            ServerConfigOptions.MasterServerConfigOptions.KEY_STORE_PASSWORD.defaultValue();

    /** The password of key manager. */
    private String keyManagerPassword =
            ServerConfigOptions.MasterServerConfigOptions.KEY_MANAGER_PASSWORD.defaultValue();

    /** The path of truststore file. */
    private String trustStorePath =
            ServerConfigOptions.MasterServerConfigOptions.TRUST_STORE_PATH.defaultValue();

    /** The password of truststore file. */
    private String trustStorePassword =
            ServerConfigOptions.MasterServerConfigOptions.TRUST_STORE_PASSWORD.defaultValue();

    private String contextPath =
            ServerConfigOptions.MasterServerConfigOptions.CONTEXT_PATH.defaultValue();

    private boolean enableDynamicPort =
            ServerConfigOptions.MasterServerConfigOptions.ENABLE_DYNAMIC_PORT.defaultValue();

    private int portRange = ServerConfigOptions.MasterServerConfigOptions.PORT_RANGE.defaultValue();

    /** Whether to enable basic authentication. */
    private boolean enableBasicAuth =
            ServerConfigOptions.MasterServerConfigOptions.ENABLE_BASIC_AUTH.defaultValue();

    /** The username for basic authentication. */
    private String basicAuthUsername =
            ServerConfigOptions.MasterServerConfigOptions.BASIC_AUTH_USERNAME.defaultValue();

    /** The password for basic authentication. */
    private String basicAuthPassword =
            ServerConfigOptions.MasterServerConfigOptions.BASIC_AUTH_PASSWORD.defaultValue();

    public void setPort(int port) {
        checkPositive(port, ServerConfigOptions.MasterServerConfigOptions.HTTP + " must be > 0");
        this.port = port;
    }
}
