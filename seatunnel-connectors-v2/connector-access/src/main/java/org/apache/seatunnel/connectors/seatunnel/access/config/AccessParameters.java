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

package org.apache.seatunnel.connectors.seatunnel.access.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Setter
@Getter
public class AccessParameters implements Serializable {
    private String driver;
    private String url;
    private String username;
    private String password;
    private String query;
    private List<String> fields;
    private String table;
    private Integer batchSize;

    public void buildWithConfig(Config config) {
        this.driver = config.getString(AccessConfig.DRIVER.key());
        this.url = config.getString(AccessConfig.URL.key());

        if (config.hasPath(AccessConfig.USERNAME.key())) {
            this.username = config.getString(AccessConfig.USERNAME.key());
        }
        if (config.hasPath(AccessConfig.PASSWORD.key())) {
            this.password = config.getString(AccessConfig.PASSWORD.key());
        }
        if (config.hasPath(AccessConfig.QUERY.key())) {
            this.query = config.getString(AccessConfig.QUERY.key());
        }
        if (config.hasPath(AccessConfig.FIELDS.key())) {
            this.fields = config.getStringList(AccessConfig.FIELDS.key());
        }
        if (config.hasPath(AccessConfig.TABLE.key())) {
            this.table = config.getString(AccessConfig.TABLE.key());
        }
        if (config.hasPath(AccessConfig.BATCH_SIZE.key())) {
            this.batchSize = config.getInt(AccessConfig.BATCH_SIZE.key());
        } else {
            this.batchSize = AccessConfig.BATCH_SIZE.defaultValue();
        }
    }
}
