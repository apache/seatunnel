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

package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions.USERNAME;

@Getter
@ToString
@AllArgsConstructor
public class StarRocksConfig implements Serializable {

    private List<String> nodeUrls;
    private String username;
    private String password;
    private String database;
    private String table;

    public StarRocksConfig(ReadonlyConfig config) {
        this.nodeUrls = config.get(NODE_URLS);
        this.username = config.get(USERNAME);
        this.password = config.get(PASSWORD);
        this.database = config.get(DATABASE);
        this.table = config.get(TABLE);
    }
}
