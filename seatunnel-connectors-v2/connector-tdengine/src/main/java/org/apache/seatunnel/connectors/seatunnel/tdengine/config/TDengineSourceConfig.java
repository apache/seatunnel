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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class TDengineSourceConfig implements Serializable {

    /** jdbc:TAOS-RS://localhost:6041/ */
    private String url;

    private String username;
    private String password;
    private String database;
    private String stable;
    private String lowerBound;
    private String upperBound;
    private List<String> fields;
    private List<String> tags;

    public static TDengineSourceConfig buildSourceConfig(ReadonlyConfig pluginConfig) {
        TDengineSourceConfig tdengineSourceConfig = new TDengineSourceConfig();
        tdengineSourceConfig.setUrl(pluginConfig.get(TDengineSourceOptions.URL));
        tdengineSourceConfig.setDatabase(pluginConfig.get(TDengineSourceOptions.DATABASE));
        tdengineSourceConfig.setStable(pluginConfig.get(TDengineSourceOptions.STABLE));
        tdengineSourceConfig.setUsername(pluginConfig.get(TDengineSourceOptions.USERNAME));
        tdengineSourceConfig.setPassword(pluginConfig.get(TDengineSourceOptions.PASSWORD));
        tdengineSourceConfig.setUpperBound(pluginConfig.get(TDengineSourceOptions.UPPER_BOUND));
        tdengineSourceConfig.setLowerBound(pluginConfig.get(TDengineSourceOptions.LOWER_BOUND));
        return tdengineSourceConfig;
    }
}
