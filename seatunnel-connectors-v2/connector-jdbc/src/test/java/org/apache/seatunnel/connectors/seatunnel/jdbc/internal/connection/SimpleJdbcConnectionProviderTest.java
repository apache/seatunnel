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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Tests for driver disambiguation when multiple JDBC drivers are registered.
 *
 * <p>This test validates the concept behind the fix for issue #10242: when both PostgreSQL and
 * OpenGauss JDBC drivers are on the classpath, the {@link DriverManager} must correctly select the
 * driver that accepts the given URL. The fix in {@link SimpleJdbcConnectionProvider} and {@link
 * org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog} uses {@link
 * Driver#acceptsURL(String)} to verify the driver before connecting, and falls back to {@link
 * DriverManager#getDriver(String)} when the loaded driver does not accept the URL.
 */
class SimpleJdbcConnectionProviderTest {

    private Driver postgresDriver;
    private Driver openGaussDriver;

    @BeforeEach
    void setUp() throws SQLException {
        // Register a mock PostgreSQL driver that accepts jdbc:postgresql:// URLs
        postgresDriver = new UrlPrefixDriver("org.postgresql.Driver", "jdbc:postgresql://");
        DriverManager.registerDriver(postgresDriver);

        // Register a mock OpenGauss driver that accepts jdbc:opengauss:// URLs
        openGaussDriver = new UrlPrefixDriver("org.opengauss.Driver", "jdbc:opengauss://");
        DriverManager.registerDriver(openGaussDriver);
    }

    @AfterEach
    void tearDown() throws SQLException {
        DriverManager.deregisterDriver(postgresDriver);
        DriverManager.deregisterDriver(openGaussDriver);
    }

    @Test
    void testDriverManagerSelectsCorrectDriverByUrl() throws SQLException {
        // When querying for a PostgreSQL URL, DriverManager should return a driver that
        // accepts the URL. We use acceptsURL instead of assertSame because pre-existing
        // drivers (e.g., auto-loaded via ServiceLoader) may be registered alongside mock
        // drivers, and DriverManager.getDriver() returns the first matching driver.
        Driver driver = DriverManager.getDriver("jdbc:postgresql://localhost:5432/test");
        Assertions.assertNotNull(driver);
        Assertions.assertTrue(
                driver.acceptsURL("jdbc:postgresql://localhost:5432/test"),
                "Driver should accept PostgreSQL URL");
        Assertions.assertFalse(
                driver.acceptsURL("jdbc:opengauss://localhost:5432/test"),
                "Driver should NOT accept OpenGauss URL");

        // When querying for an OpenGauss URL, DriverManager should return a driver that
        // accepts the URL.
        driver = DriverManager.getDriver("jdbc:opengauss://localhost:5432/test");
        Assertions.assertNotNull(driver);
        Assertions.assertTrue(
                driver.acceptsURL("jdbc:opengauss://localhost:5432/test"),
                "Driver should accept OpenGauss URL");
        Assertions.assertFalse(
                driver.acceptsURL("jdbc:postgresql://localhost:5432/test"),
                "Driver should NOT accept PostgreSQL URL");
    }

    @Test
    void testAcceptsUrlCorrectlyRejectsIncompatibleDriver() throws SQLException {
        // PostgreSQL driver should accept PostgreSQL URLs
        Assertions.assertTrue(postgresDriver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
        // PostgreSQL driver should NOT accept OpenGauss URLs
        Assertions.assertFalse(postgresDriver.acceptsURL("jdbc:opengauss://localhost:5432/test"));

        // OpenGauss driver should accept OpenGauss URLs
        Assertions.assertTrue(openGaussDriver.acceptsURL("jdbc:opengauss://localhost:5432/test"));
        // OpenGauss driver should NOT accept PostgreSQL URLs
        Assertions.assertFalse(openGaussDriver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
    }

    @Test
    void testGetOrEstablishConnectionFallsBackWhenDriverRejectsUrl() throws Exception {
        // Register a driver that rejects all URLs via acceptsURL() returning false
        RejectingDriver rejectingDriver = new RejectingDriver();
        DriverManager.registerDriver(rejectingDriver);

        // Register a mock driver that accepts the URL and returns a mock Connection
        Driver acceptingDriver = Mockito.mock(Driver.class);
        Mockito.when(acceptingDriver.acceptsURL(Mockito.anyString())).thenReturn(true);
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(acceptingDriver.connect(Mockito.anyString(), Mockito.any(Properties.class)))
                .thenReturn(mockConnection);
        DriverManager.registerDriver(acceptingDriver);

        try {
            JdbcConnectionConfig config =
                    JdbcConnectionConfig.builder()
                            .url("jdbc:test://localhost/test")
                            .driverName(RejectingDriver.class.getName())
                            .build();

            SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(config);
            Connection result = provider.getOrEstablishConnection();

            Assertions.assertNotNull(result);
            Assertions.assertSame(mockConnection, result);
        } finally {
            DriverManager.deregisterDriver(rejectingDriver);
            DriverManager.deregisterDriver(acceptingDriver);
        }
    }

    @Test
    void testGetOrEstablishConnectionFallsBackWhenDriverThrowsOnAcceptsUrl() throws Exception {
        // Register a driver that throws SQLException from acceptsURL()
        ThrowingDriver throwingDriver = new ThrowingDriver();
        DriverManager.registerDriver(throwingDriver);

        // Register a mock driver that accepts the URL and returns a mock Connection
        Driver acceptingDriver = Mockito.mock(Driver.class);
        Mockito.when(acceptingDriver.acceptsURL(Mockito.anyString())).thenReturn(true);
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(acceptingDriver.connect(Mockito.anyString(), Mockito.any(Properties.class)))
                .thenReturn(mockConnection);
        DriverManager.registerDriver(acceptingDriver);

        try {
            JdbcConnectionConfig config =
                    JdbcConnectionConfig.builder()
                            .url("jdbc:test://localhost/test")
                            .driverName(ThrowingDriver.class.getName())
                            .build();

            SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(config);
            Connection result = provider.getOrEstablishConnection();

            Assertions.assertNotNull(result);
            Assertions.assertSame(mockConnection, result);
        } finally {
            DriverManager.deregisterDriver(throwingDriver);
            DriverManager.deregisterDriver(acceptingDriver);
        }
    }

    /**
     * A simple {@link Driver} implementation that accepts URLs starting with a given prefix and
     * reports its class name via {@link #getClass()}.
     */
    private static class UrlPrefixDriver implements Driver {
        private final String className;
        private final String urlPrefix;

        UrlPrefixDriver(String className, String urlPrefix) {
            this.className = className;
            this.urlPrefix = urlPrefix;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(urlPrefix);
        }

        @Override
        public Connection connect(String url, Properties info) {
            // Not needed for these tests — no real connection is established
            return null;
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
