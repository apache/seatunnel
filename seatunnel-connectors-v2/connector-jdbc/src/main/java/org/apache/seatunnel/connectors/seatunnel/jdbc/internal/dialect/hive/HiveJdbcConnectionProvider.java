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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.NonNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode.KERBEROS_AUTHENTICATION_FAILED;

public class HiveJdbcConnectionProvider extends SimpleJdbcConnectionProvider {

    public HiveJdbcConnectionProvider(@NonNull JdbcConnectionConfig jdbcConfig) {
        super(jdbcConfig);
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            return super.getConnection();
        }
        JdbcConnectionConfig jdbcConfig = super.getJdbcConfig();
        if (jdbcConfig.useKerberos) {
            System.setProperty("java.security.krb5.conf", jdbcConfig.krb5Path);
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                UserGroupInformation.loginUserFromKeytab(
                        jdbcConfig.kerberosPrincipal, jdbcConfig.kerberosKeytabPath);
            } catch (IOException e) {
                throw new JdbcConnectorException(KERBEROS_AUTHENTICATION_FAILED, e);
            }
        }
        Driver driver = getLoadedDriver();
        Properties info = new Properties();
        if (super.getJdbcConfig().getUsername().isPresent()) {
            info.setProperty("user", super.getJdbcConfig().getUsername().get());
        }
        if (super.getJdbcConfig().getPassword().isPresent()) {
            info.setProperty("password", super.getJdbcConfig().getPassword().get());
        }
        super.setConnection(driver.connect(super.getJdbcConfig().getUrl(), info));
        if (super.getConnection() == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                    "No suitable driver found for " + super.getJdbcConfig().getUrl());
        }
        return super.getConnection();
    }
}
