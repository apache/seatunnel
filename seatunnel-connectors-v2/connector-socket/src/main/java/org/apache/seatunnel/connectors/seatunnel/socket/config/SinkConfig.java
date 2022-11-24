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

import static org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkConfigOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkConfigOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkConfigOptions.PORT;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SinkConfig implements Serializable {
    private String host;
    private int port;
    private int maxNumRetries;

    public SinkConfig(Config config) {
        this.host = config.getString(HOST.key());
        this.port = config.getInt(PORT.key());
        if (config.hasPath(MAX_RETRIES.key())) {
            this.maxNumRetries = config.getInt(MAX_RETRIES.key());
        }
    }
}
