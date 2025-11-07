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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle;

import org.apache.commons.lang3.StringUtils;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/** Copy from testcontainers. */
public class OracleContainer extends JdbcDatabaseContainer<OracleContainer> {

    public static final String NAME = "oracle";

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("goodboy008/oracle-19.3.0-ee");

    static final String DEFAULT_TAG = "latest";

    static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

    static final int ORACLE_PORT = 1521;

    private static final int APEX_HTTP_PORT = 8080;

    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;

    // Container defaults
    static final String DEFAULT_DATABASE_NAME = "xepdb1";

    static final String DEFAULT_SID = "ORCLCDB";

    static final String DEFAULT_SYSTEM_USER = "system";

    static final String DEFAULT_SYS_USER = "sys";

    // Test container defaults
    static final String APP_USER = "test";

    static final String APP_USER_PASSWORD = "system";

    // Restricted user and database names
    private static final List<String> ORACLE_SYSTEM_USERS =
            Arrays.asList(DEFAULT_SYSTEM_USER, DEFAULT_SYS_USER);

    private String databaseName = DEFAULT_DATABASE_NAME;

    private String username = APP_USER;

    private String password = APP_USER_PASSWORD;

    private boolean usingSid = false;

    /** @deprecated use @link OracleContainer(DockerImageName) instead */
    @Deprecated
    public OracleContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public OracleContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OracleContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        preconfigure();
    }

    public OracleContainer(Future<String> dockerImageName) {
        super(dockerImageName);
        preconfigure();
    }

    private void preconfigure() {
        this.waitStrategy =
                new LogMessageWaitStrategy()
                        .withRegEx(".*DATABASE IS READY TO USE!.*\\s")
                        .withTimes(1)
                        .withStartupTimeout(
                                Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS));

        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        addExposedPorts(ORACLE_PORT, APEX_HTTP_PORT);
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }

    @NotNull @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(getMappedPort(ORACLE_PORT));
    }

    @Override
    public String getDriverClassName() {
        return "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    public String getJdbcUrl() {
        return isUsingSid()
                ? "jdbc:oracle:thin:" + "@" + getHost() + ":" + getOraclePort() + ":" + getSid()
                : "jdbc:oracle:thin:"
                        + "@"
                        + getHost()
                        + ":"
                        + getOraclePort()
                        + "/"
                        + getDatabaseName();
    }

    @Override
    public String getUsername() {
        // An application user is tied to the database, and therefore not authenticated to connect
        // to SID.
        return isUsingSid() ? DEFAULT_SYSTEM_USER : username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    protected boolean isUsingSid() {
        return usingSid;
    }

    @Override
    public OracleContainer withUsername(String username) {
        if (StringUtils.isEmpty(username)) {
            throw new IllegalArgumentException("Username cannot be null or empty");
        }
        if (ORACLE_SYSTEM_USERS.contains(username.toLowerCase())) {
            throw new IllegalArgumentException("Username cannot be one of " + ORACLE_SYSTEM_USERS);
        }
        this.username = username;
        return self();
    }

    @Override
    public OracleContainer withPassword(String password) {
        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Password cannot be null or empty");
        }
        this.password = password;
        return self();
    }

    @Override
    public OracleContainer withDatabaseName(String databaseName) {
        if (StringUtils.isEmpty(databaseName)) {
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }

        if (DEFAULT_DATABASE_NAME.equals(databaseName.toLowerCase())) {
            throw new IllegalArgumentException(
                    "Database name cannot be set to " + DEFAULT_DATABASE_NAME);
        }

        this.databaseName = databaseName;
        return self();
    }

    public OracleContainer usingSid() {
        this.usingSid = true;
        return self();
    }

    @Override
    public OracleContainer withUrlParam(String paramName, String paramValue) {
        throw new UnsupportedOperationException("The Oracle Database driver does not support this");
    }

    @SuppressWarnings("SameReturnValue")
    public String getSid() {
        return DEFAULT_SID;
    }

    public Integer getOraclePort() {
        return getMappedPort(ORACLE_PORT);
    }

    @SuppressWarnings("unused")
    public Integer getWebPort() {
        return getMappedPort(APEX_HTTP_PORT);
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1 FROM DUAL";
    }

    @Override
    protected void configure() {
        withEnv("ORACLE_PASSWORD", password);

        // Only set ORACLE_DATABASE if different than the default.
        if (databaseName != DEFAULT_DATABASE_NAME) {
            withEnv("ORACLE_DATABASE", databaseName);
        }

        withEnv("APP_USER", username);
        withEnv("APP_USER_PASSWORD", password);
    }
}
