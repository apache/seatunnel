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
    private static final int SESSION_TIMEOUT = 10000;
    private static final int REQUEST_TIMEOUT = 60000;
    private String zkHost;
    private String zkPath;
    private String host;
    private int port;
    private int sessionTimeout = SESSION_TIMEOUT;
    private int requestTimeout = REQUEST_TIMEOUT;
    private Boolean clusterMode;
    private String database;
    private String sql;

    private OpenMldbParameters() {
        // do nothing
    }

    public static OpenMldbParameters buildWithConfig(Config pluginConfig) {
        OpenMldbParameters openMldbParameters = new OpenMldbParameters();
        openMldbParameters.clusterMode = pluginConfig.getBoolean(OpenMldbConfig.CLUSTER_MODE);
        openMldbParameters.database = pluginConfig.getString(OpenMldbConfig.DATABASE);
        openMldbParameters.sql = pluginConfig.getString(OpenMldbConfig.SQL);
        // set zkHost
        if (pluginConfig.hasPath(OpenMldbConfig.ZK_HOST)) {
            openMldbParameters.zkHost = pluginConfig.getString(OpenMldbConfig.ZK_HOST);
        }
        // set zkPath
        if (pluginConfig.hasPath(OpenMldbConfig.ZK_PATH)) {
            openMldbParameters.zkPath = pluginConfig.getString(OpenMldbConfig.ZK_PATH);
        }
        // set host
        if (pluginConfig.hasPath(OpenMldbConfig.HOST)) {
            openMldbParameters.host = pluginConfig.getString(OpenMldbConfig.HOST);
        }
        // set port
        if (pluginConfig.hasPath(OpenMldbConfig.PORT)) {
            openMldbParameters.port = pluginConfig.getInt(OpenMldbConfig.PORT);
        }
        // set session timeout
        if (pluginConfig.hasPath(OpenMldbConfig.SESSION_TIMEOUT)) {
            openMldbParameters.sessionTimeout = pluginConfig.getInt(OpenMldbConfig.SESSION_TIMEOUT);
        }
        // set request timeout
        if (pluginConfig.hasPath(OpenMldbConfig.REQUEST_TIMEOUT)) {
            openMldbParameters.requestTimeout = pluginConfig.getInt(OpenMldbConfig.REQUEST_TIMEOUT);
        }
        return openMldbParameters;
    }
}
