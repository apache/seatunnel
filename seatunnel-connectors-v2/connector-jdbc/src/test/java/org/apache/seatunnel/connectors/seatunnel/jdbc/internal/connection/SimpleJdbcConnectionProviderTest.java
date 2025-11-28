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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

class SimpleJdbcConnectionProviderTest {

    @Test
    void testServerTimeZonePropertyAppliedForMySql() throws Exception {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:mysql://localhost:3306/test")
                        .driverName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .properties(Collections.singletonMap("foo", "bar"))
                        .build();

        TestProvider provider = new TestProvider(config);
        provider.getOrEstablishConnection();

        Properties info = provider.getLastInfo();
        Assertions.assertNotNull(info, "Driver should have been called with properties");
        Assertions.assertEquals("UTC", info.getProperty("serverTimezone"));
        Assertions.assertEquals("bar", info.getProperty("foo"));
    }

    @Test
    void testExistingServerTimezoneNotOverridden() throws Exception {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:mysql://localhost:3306/test")
                        .driverName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .properties(Collections.singletonMap("serverTimezone", "Asia/Shanghai"))
                        .build();

        TestProvider provider = new TestProvider(config);
        provider.getOrEstablishConnection();

        Properties info = provider.getLastInfo();
        Assertions.assertNotNull(info, "Driver should have been called with properties");
        // user defined property should win
        Assertions.assertEquals("Asia/Shanghai", info.getProperty("serverTimezone"));
    }

    /** Simple provider that injects a test driver recording the used properties. */
    private static class TestProvider extends SimpleJdbcConnectionProvider {
        private Properties lastInfo;

        TestProvider(JdbcConnectionConfig jdbcConfig) {
            super(jdbcConfig);
        }

        Properties getLastInfo() {
            return lastInfo;
        }

        @Override
        protected Driver getLoadedDriver() {
            return new Driver() {
                @Override
                public Connection connect(String url, Properties info) throws SQLException {
                    lastInfo = info;
                    Connection conn = Mockito.mock(Connection.class);
                    Mockito.when(conn.isValid(Mockito.anyInt())).thenReturn(true);
                    return conn;
                }

                @Override
                public boolean acceptsURL(String url) {
                    return url != null && url.startsWith("jdbc:mysql:");
                }

                @Override
                public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
                    return new java.sql.DriverPropertyInfo[0];
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
                public boolean jdbcCompliant() {
                    return false;
                }

                @Override
                public java.util.logging.Logger getParentLogger() {
                    return java.util.logging.Logger.getAnonymousLogger();
                }
            };
        }
    }
}
