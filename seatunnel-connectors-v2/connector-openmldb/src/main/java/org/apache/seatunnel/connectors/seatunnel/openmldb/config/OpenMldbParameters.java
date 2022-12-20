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

package org.apache.seatunnel.connectors.seatunnel.openmldb.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class OpenMldbParameters implements Serializable {
    private String zkHost;
    private String zkPath;
    private String host;
    private int port;
    private int sessionTimeout = OpenMldbConfig.SESSION_TIMEOUT.defaultValue();
    private int requestTimeout = OpenMldbConfig.REQUEST_TIMEOUT.defaultValue();
    private Boolean clusterMode;
    private String database;
    private String sql;

    private OpenMldbParameters() {
        // do nothing
    }

    public static OpenMldbParameters buildWithConfig(Config pluginConfig) {
        OpenMldbParameters openMldbParameters = new OpenMldbParameters();
        openMldbParameters.clusterMode = pluginConfig.getBoolean(OpenMldbConfig.CLUSTER_MODE.key());
        openMldbParameters.database = pluginConfig.getString(OpenMldbConfig.DATABASE.key());
        openMldbParameters.sql = pluginConfig.getString(OpenMldbConfig.SQL.key());
        // set zkHost
        if (pluginConfig.hasPath(OpenMldbConfig.ZK_HOST.key())) {
            openMldbParameters.zkHost = pluginConfig.getString(OpenMldbConfig.ZK_HOST.key());
        }
        // set zkPath
        if (pluginConfig.hasPath(OpenMldbConfig.ZK_PATH.key())) {
            openMldbParameters.zkPath = pluginConfig.getString(OpenMldbConfig.ZK_PATH.key());
        }
        // set host
        if (pluginConfig.hasPath(OpenMldbConfig.HOST.key())) {
            openMldbParameters.host = pluginConfig.getString(OpenMldbConfig.HOST.key());
        }
        // set port
        if (pluginConfig.hasPath(OpenMldbConfig.PORT.key())) {
            openMldbParameters.port = pluginConfig.getInt(OpenMldbConfig.PORT.key());
        }
        // set session timeout
        if (pluginConfig.hasPath(OpenMldbConfig.SESSION_TIMEOUT.key())) {
            openMldbParameters.sessionTimeout = pluginConfig.getInt(OpenMldbConfig.SESSION_TIMEOUT.key());
        }
        // set request timeout
        if (pluginConfig.hasPath(OpenMldbConfig.REQUEST_TIMEOUT.key())) {
            openMldbParameters.requestTimeout = pluginConfig.getInt(OpenMldbConfig.REQUEST_TIMEOUT.key());
        }
        return openMldbParameters;
    }
}
