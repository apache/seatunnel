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

package org.apache.seatunnel.e2e.connector.edgesocket;

import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Set;

class EdgeSocketMySqlContainer extends JdbcDatabaseContainer<EdgeSocketMySqlContainer> {
    private static final String IMAGE = "mysql";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_ROOT_USER = "root";
    private static final String MY_CNF_CONFIG_OVERRIDE_PARAM_NAME = "MY_CNF";
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";

    private String databaseName = "test";
    private String username = "test";
    private String password = "test";

    EdgeSocketMySqlContainer(String version) {
        super(DockerImageName.parse(IMAGE + ":" + version));
        addExposedPort(MYSQL_PORT);
    }

    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return Collections.singleton(getMappedPort(MYSQL_PORT));
    }

    @Override
    protected void configure() {
        optionallyMapResourceParameterAsVolume(
                MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, "/etc/mysql/my.cnf", "mysql-default-conf");
        optionallyMapResourceParameterAsVolume(
                SETUP_SQL_PARAM_NAME, "/docker-entrypoint-initdb.d/setup.sql", "N/A");

        addEnv("MYSQL_DATABASE", databaseName);
        if (MYSQL_ROOT_USER.equalsIgnoreCase(username)) {
            if (password != null && !password.isEmpty()) {
                addEnv("MYSQL_ROOT_PASSWORD", password);
            } else {
                addEnv("MYSQL_ALLOW_EMPTY_PASSWORD", "yes");
            }
        } else {
            addEnv("MYSQL_USER", username);
            if (password != null && !password.isEmpty()) {
                addEnv("MYSQL_PASSWORD", password);
                addEnv("MYSQL_ROOT_PASSWORD", password);
            } else {
                throw new ContainerLaunchException(
                        "Empty password can be used only with the root user");
            }
        }
        setStartupAttempts(3);
    }

    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    int getDatabasePort() {
        return getMappedPort(MYSQL_PORT);
    }

    @Override
    protected String constructUrlForConnection(String queryString) {
        String url = super.constructUrlForConnection(queryString);
        if (!url.contains("useSSL=")) {
            String separator = url.contains("?") ? "&" : "?";
            url = url + separator + "useSSL=false";
        }
        if (!url.contains("allowPublicKeyRetrieval=")) {
            url = url + "&allowPublicKeyRetrieval=true";
        }
        return url;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    EdgeSocketMySqlContainer withConfigurationOverride(String path) {
        parameters.put(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, path);
        return this;
    }

    EdgeSocketMySqlContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    @Override
    public EdgeSocketMySqlContainer withDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public EdgeSocketMySqlContainer withUsername(String username) {
        this.username = username;
        return this;
    }

    @Override
    public EdgeSocketMySqlContainer withPassword(String password) {
        this.password = password;
        return this;
    }
}
