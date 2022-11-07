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

package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Objects;

public class SocketSourceParameter implements Serializable {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9999;
    private String host;
    private Integer port;

    public static final Option<String> HOST =
        Options.key("host").stringType().defaultValue(DEFAULT_HOST).withDescription("socket host");

    public static final Option<Integer> PORT =
        Options.key("port").intType().defaultValue(DEFAULT_PORT).withDescription("socket port");

    public String getHost() {
        return StringUtils.isBlank(host) ? DEFAULT_HOST : host;
    }

    public Integer getPort() {
        return Objects.isNull(port) ? DEFAULT_PORT : port;
    }

    public SocketSourceParameter(Config config) {
        if (config.hasPath(HOST.key())) {
            this.host = config.getString(HOST.key());
        } else {
            this.host = DEFAULT_HOST;
        }

        if (config.hasPath(PORT.key())) {
            this.port = config.getInt(PORT.key());
        } else {
            this.port = DEFAULT_PORT;
        }
    }
}
