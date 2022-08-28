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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SinkConfig implements Serializable {
    public static final String HOST = "host";
    public static final String PORT = "port";
    private static final String MAX_RETRIES = "max_retries";
    private static final int DEFAULT_MAX_RETRIES = 3;
    private String host;
    private int port;
    private Integer maxNumRetries = DEFAULT_MAX_RETRIES;

    public SinkConfig(Config config) {
        this.host = config.getString(HOST);
        this.port = config.getInt(PORT);
        if (config.hasPath(MAX_RETRIES)) {
            this.maxNumRetries = config.getInt(MAX_RETRIES);
        }
    }
}
