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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.testutils;

import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.HashSet;
import java.util.Set;

/** Docker container for MariaDB. */
@SuppressWarnings("MagicNumber")
public class MariaDbContainer extends JdbcDatabaseContainer<MariaDbContainer> {

    public static final String IMAGE = "mariadb";
    public static final Integer MARIADB_PORT = 3306;

    private static final String MY_CNF_CONFIG_OVERRIDE_PARAM_NAME = "MY_CNF";
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";
    private static final String MARIADB_ROOT_USER = "root";
    /** File mode that keeps copied init SQL readable for the container entrypoint. */
    private static final int SETUP_SQL_FILE_MODE = 0644;

    private String databaseName = "test";
    private String username = "test";
    private String password = "test";

    public MariaDbContainer() {
        this(MariaDbVersion.V10_11);
    }

    public MariaDbContainer(MariaDbVersion version) {
        super(DockerImageName.parse(IMAGE + ":" + version.getVersion()));
        addExposedPort(MARIADB_PORT);
    }

    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(MARIADB_PORT));
    }

    @Override
    protected void configure() {
        withCommand(
                "--log-bin=mariadb-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--server-id=223344",
                "--gtid-domain-id=1");

        if (parameters.containsKey(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME)) {
            String configPath = parameters.get(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME);
            withCopyFileToContainer(
                    MountableFile.forClasspathResource(configPath, SETUP_SQL_FILE_MODE),
                    "/etc/mysql/conf.d/my.cnf");
            withCopyFileToContainer(
                    MountableFile.forClasspathResource(configPath, SETUP_SQL_FILE_MODE),
                    "/etc/mysql/mariadb.conf.d/my.cnf");
        }

        if (parameters.containsKey(SETUP_SQL_PARAM_NAME)) {
            copyResourceToInitDirectory(parameters.get(SETUP_SQL_PARAM_NAME));
        }

        addEnv("MARIADB_DATABASE", databaseName);
        addEnv("MARIADB_USER", username);
        if (password != null && !password.isEmpty()) {
            addEnv("MARIADB_PASSWORD", password);
            addEnv("MARIADB_ROOT_PASSWORD", password);
        } else if (MARIADB_ROOT_USER.equalsIgnoreCase(username)) {
            addEnv("MARIADB_ALLOW_EMPTY_ROOT_PASSWORD", "yes");
        } else {
            throw new ContainerLaunchException(
                    "Empty password can be used only with the root user");
        }
        setStartupAttempts(3);
    }

    @Override
    public String getDriverClassName() {
        return "org.mariadb.jdbc.Driver";
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mariadb://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(MARIADB_PORT);
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

    public MariaDbContainer withConfigurationOverride(String s) {
        parameters.put(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, s);
        withCopyFileToContainer(
                MountableFile.forClasspathResource(s, SETUP_SQL_FILE_MODE),
                "/etc/mysql/conf.d/my.cnf");
        withCopyFileToContainer(
                MountableFile.forClasspathResource(s, SETUP_SQL_FILE_MODE),
                "/etc/mysql/mariadb.conf.d/my.cnf");
        return this;
    }

    public MariaDbContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    private void copyResourceToInitDirectory(String setupSqlPath) {
        String targetPath =
                "/docker-entrypoint-initdb.d/"
                        + setupSqlPath.substring(setupSqlPath.lastIndexOf('/') + 1);
        withCopyFileToContainer(
                MountableFile.forClasspathResource(setupSqlPath, SETUP_SQL_FILE_MODE), targetPath);
    }

    @Override
    public MariaDbContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public MariaDbContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    @Override
    public MariaDbContainer withPassword(final String password) {
        this.password = password;
        return this;
    }
}
