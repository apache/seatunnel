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

import static org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkConfigOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkConfigOptions.PORT;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Objects;

public class SocketSourceParameter implements Serializable {
    private final String host;
    private final Integer port;

    public String getHost() {
        return StringUtils.isBlank(host) ? HOST.defaultValue() : host;
    }

    public Integer getPort() {
        return Objects.isNull(port) ? PORT.defaultValue() : port;
    }

    public SocketSourceParameter(Config config) {
        if (config.hasPath(HOST.key())) {
            this.host = config.getString(HOST.key());
        } else {
            this.host = HOST.defaultValue();
        }

        if (config.hasPath(PORT.key())) {
            this.port = config.getInt(PORT.key());
        } else {
            this.port = PORT.defaultValue();
        }
    }
}
