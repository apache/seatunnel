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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConfig.buildJdbcConnectionOptions;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class JdbcSinkOptions implements Serializable {
    private JdbcConnectionOptions jdbcConnectionOptions;
    private boolean isExactlyOnce;
    public String simpleSQL;
    private String table;
    private List<String> primaryKeys;
    public JdbcSinkOptions(Config config) {
        this.jdbcConnectionOptions = buildJdbcConnectionOptions(config);
        if (config.hasPath(JdbcConfig.IS_EXACTLY_ONCE.key()) && config.getBoolean(JdbcConfig.IS_EXACTLY_ONCE.key())) {
            this.isExactlyOnce = true;
        }

        if (config.hasPath(JdbcConfig.TABLE.key())) {
            this.table = config.getString(JdbcConfig.TABLE.key());
            if (config.hasPath(JdbcConfig.PRIMARY_KEYS.key())) {
                this.primaryKeys = config.getStringList(JdbcConfig.PRIMARY_KEYS.key());
            }
        } else {
            this.simpleSQL = config.getString(JdbcConfig.QUERY.key());
        }
    }
}
