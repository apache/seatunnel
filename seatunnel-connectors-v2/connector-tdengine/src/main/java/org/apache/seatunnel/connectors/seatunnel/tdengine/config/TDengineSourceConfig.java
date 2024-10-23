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

package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.STABLE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.TIMEZONE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.URL;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.USERNAME;

@Data
public class TDengineSourceConfig implements Serializable {

    /** jdbc:TAOS-RS://localhost:6041/ */
    private String url;

    private String username;
    private String password;
    private String database;
    private String stable;
    // param of timezone in 'jdbc:TAOS-RS' just effect on taosadapter side, other than the JDBC
    // client side
    // so this param represent the server-side timezone setting up
    private String timezone;
    private String lowerBound;
    private String upperBound;
    private List<String> fields;
    private List<String> tags;

    public static TDengineSourceConfig buildSourceConfig(Config pluginConfig) {
        TDengineSourceConfig tdengineSourceConfig = new TDengineSourceConfig();
        tdengineSourceConfig.setUrl(
                pluginConfig.hasPath(ConfigNames.URL)
                        ? pluginConfig.getString(ConfigNames.URL)
                        : null);
        tdengineSourceConfig.setDatabase(
                pluginConfig.hasPath(DATABASE) ? pluginConfig.getString(DATABASE) : null);
        tdengineSourceConfig.setStable(
                pluginConfig.hasPath(STABLE) ? pluginConfig.getString(STABLE) : null);
        tdengineSourceConfig.setUsername(
                pluginConfig.hasPath(USERNAME) ? pluginConfig.getString(USERNAME) : null);
        tdengineSourceConfig.setPassword(
                pluginConfig.hasPath(PASSWORD) ? pluginConfig.getString(PASSWORD) : null);
        tdengineSourceConfig.setUpperBound(
                pluginConfig.hasPath(UPPER_BOUND) ? pluginConfig.getString(UPPER_BOUND) : null);
        tdengineSourceConfig.setLowerBound(
                pluginConfig.hasPath(LOWER_BOUND) ? pluginConfig.getString(LOWER_BOUND) : null);
        tdengineSourceConfig.setTimezone(
                pluginConfig.hasPath(TIMEZONE) ? pluginConfig.getString(TIMEZONE) : "UTC");
        tdengineSourceConfig.setFields(
                pluginConfig.hasPath(FIELDS)
                        ? pluginConfig.getStringList(FIELDS)
                        : new ArrayList<>());

        return tdengineSourceConfig;
    }

    public static class ConfigNames {

        public static String URL = "url";
        public static String USERNAME = "username";
        public static String PASSWORD = "password";
        public static String DATABASE = "database";
        public static String STABLE = "stable";
        public static String TIMEZONE = "timezone";
        public static String LOWER_BOUND = "lower_bound";
        public static String UPPER_BOUND = "upper_bound";
        public static String FIELDS = "fields";
    }
}
