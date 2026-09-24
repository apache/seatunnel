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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase;

import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashSet;
import java.util.Set;

/**
 * MySQL-compatible source container used by the OceanBase CDC E2E module.
 *
 * <p>The utility stays local to this module so OceanBase E2E does not depend on MySQL CDC test
 * artifacts while still preserving docker-entrypoint initialization for GTID and binlog settings.
 */
class MySqlCompatibleContainer extends JdbcDatabaseContainer<MySqlCompatibleContainer> {

    /** Official MySQL image repository used as the OceanBase-compatible CDC runtime. */
    private static final String IMAGE = "mysql";

    /** MySQL service port exposed inside the Docker network. */
    private static final Integer MYSQL_PORT = 3306;

    /** Classpath resource parameter that maps a custom my.cnf into the container. */
    private static final String MY_CNF_CONFIG_OVERRIDE_PARAM_NAME = "MY_CNF";

    /** Classpath resource parameter that maps setup SQL into docker-entrypoint-initdb.d. */
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";

    /** MySQL root user name, used to decide whether an empty password is legal. */
    private static final String MYSQL_ROOT_USER = "root";

    /** Database created by the container entrypoint before setup SQL runs. */
    private String databaseName = "test";

    /** User created by the container entrypoint for JDBC setup and assertions. */
    private String username = "test";

    /** Password used by both the test user and the root account. */
    private String password = "test";

    /**
     * Create a MySQL-compatible container with a fixed image tag.
     *
     * @param version MySQL image tag used by the E2E test
     */
    MySqlCompatibleContainer(String version) {
        super(DockerImageName.parse(IMAGE + ":" + version));
        addExposedPort(MYSQL_PORT);
    }

    /**
     * Return the mapped MySQL port used by Testcontainers' JDBC wait strategy.
     *
     * @return mapped liveness check ports
     */
    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        Set<Integer> ports = new HashSet<>();
        ports.add(getMappedPort(MYSQL_PORT));
        return ports;
    }

    /** Configure MySQL entrypoint environment and optional classpath-mounted resources. */
    @Override
    protected void configure() {
        optionallyMapResourceParameterAsVolume(
                MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, "/etc/mysql/", "mysql-default-conf");

        if (parameters.containsKey(SETUP_SQL_PARAM_NAME)) {
            optionallyMapResourceParameterAsVolume(
                    SETUP_SQL_PARAM_NAME, "/docker-entrypoint-initdb.d/", "N/A");
        }

        addEnv("MYSQL_DATABASE", databaseName);
        addEnv("MYSQL_USER", username);
        if (password != null && !password.isEmpty()) {
            addEnv("MYSQL_PASSWORD", password);
            addEnv("MYSQL_ROOT_PASSWORD", password);
        } else if (MYSQL_ROOT_USER.equalsIgnoreCase(username)) {
            addEnv("MYSQL_ALLOW_EMPTY_PASSWORD", "yes");
        } else {
            throw new ContainerLaunchException(
                    "Empty password can be used only with the root user");
        }
        setStartupAttempts(3);
    }

    /**
     * Resolve the MySQL driver class name used by the local JDBC assertions.
     *
     * @return available MySQL driver class
     */
    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    /**
     * Build a JDBC URL for the requested database.
     *
     * @param databaseName target database name
     * @return JDBC URL for the mapped container port
     */
    String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    /**
     * Build the default JDBC URL for this container.
     *
     * @return JDBC URL for the configured database
     */
    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    /**
     * Return the host-mapped MySQL port.
     *
     * @return mapped MySQL port
     */
    int getDatabasePort() {
        return getMappedPort(MYSQL_PORT);
    }

    /**
     * Add connection parameters needed by MySQL 8 test containers.
     *
     * @param queryString query string from Testcontainers
     * @return JDBC URL with required compatibility parameters
     */
    @Override
    protected String constructUrlForConnection(String queryString) {
        String url = super.constructUrlForConnection(queryString);

        if (!url.contains("useSSL=")) {
            url = appendUrlParam(url, "useSSL=false");
        }

        if (!url.contains("allowPublicKeyRetrieval=")) {
            url = appendUrlParam(url, "allowPublicKeyRetrieval=true");
        }

        return url;
    }

    /**
     * Return the configured database name.
     *
     * @return database name
     */
    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Return the configured JDBC user.
     *
     * @return JDBC user name
     */
    @Override
    public String getUsername() {
        return username;
    }

    /**
     * Return the configured JDBC password.
     *
     * @return JDBC password
     */
    @Override
    public String getPassword() {
        return password;
    }

    /**
     * Return a cheap query for container readiness checks.
     *
     * @return readiness query
     */
    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    /**
     * Set the classpath my.cnf resource that should be mounted into the container.
     *
     * @param configPath classpath path to the MySQL config file
     * @return this container
     */
    MySqlCompatibleContainer withConfigurationOverride(String configPath) {
        parameters.put(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, configPath);
        return this;
    }

    /**
     * Set the classpath setup SQL resource that should run during MySQL entrypoint initialization.
     *
     * @param sqlPath classpath path to setup SQL
     * @return this container
     */
    MySqlCompatibleContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    /**
     * Set the database created by the MySQL entrypoint.
     *
     * @param databaseName database name
     * @return this container
     */
    @Override
    public MySqlCompatibleContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Set the JDBC user created by the MySQL entrypoint.
     *
     * @param username JDBC user name
     * @return this container
     */
    @Override
    public MySqlCompatibleContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    /**
     * Set the password used by the JDBC and root users.
     *
     * @param password JDBC password
     * @return this container
     */
    @Override
    public MySqlCompatibleContainer withPassword(final String password) {
        this.password = password;
        return this;
    }

    /**
     * Append a JDBC URL parameter using the correct separator for the current URL.
     *
     * @param url current JDBC URL
     * @param parameter URL parameter to append
     * @return JDBC URL with the parameter appended
     */
    private String appendUrlParam(String url, String parameter) {
        String separator = url.contains("?") ? "&" : "?";
        return url + separator + parameter;
    }
}
