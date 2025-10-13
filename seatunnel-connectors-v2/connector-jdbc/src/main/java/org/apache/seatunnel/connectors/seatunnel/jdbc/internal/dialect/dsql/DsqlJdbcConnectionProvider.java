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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dsql;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;

import lombok.NonNull;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;
import software.amazon.awssdk.services.dsql.model.GenerateAuthTokenRequest;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class DsqlJdbcConnectionProvider extends SimpleJdbcConnectionProvider {

    private AwsCredentialsProvider provider;
    private DsqlUtilities dsqlUtilities;

    public DsqlJdbcConnectionProvider(@NonNull JdbcConnectionConfig jdbcConfig) {
        super(jdbcConfig);
        this.provider =
                new AwsCredentialsProvider() {
                    @Override
                    public AwsCredentials resolveCredentials() {
                        return AwsBasicCredentials.create(
                                jdbcConfig.getAccessKeyId(), jdbcConfig.getSecretAccessKey());
                    }
                };
        this.dsqlUtilities =
                DsqlUtilities.builder()
                        .region(Region.of(jdbcConfig.getRegion()))
                        .credentialsProvider(provider)
                        .build();
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            return connection;
        }
        Driver driver = getLoadedDriver();
        Properties info = new Properties();
        if (jdbcConfig.getUsername().isPresent()) {
            info.setProperty("user", jdbcConfig.getUsername().get());
        }
        String url = jdbcConfig.getUrl();
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(url);
        info.setProperty("password", generateAuthToken(urlInfo.getHost()));

        info.putAll(jdbcConfig.getProperties());

        connection = driver.connect(url, info);
        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                    "No suitable driver found for " + url);
        }

        connection.setAutoCommit(jdbcConfig.isAutoCommit());

        return connection;
    }

    private String generateAuthToken(String clusterEndpoint) {
        JdbcConnectionConfig jdbcConfig = super.getJdbcConfig();
        GenerateAuthTokenRequest tokenGenerator =
                GenerateAuthTokenRequest.builder()
                        .hostname(clusterEndpoint)
                        .region(Region.of(jdbcConfig.getRegion()))
                        .credentialsProvider(this.provider)
                        .build();

        if ("admin".equals(jdbcConfig.getUsername().get())) {
            return dsqlUtilities.generateDbConnectAdminAuthToken(tokenGenerator);
        } else {
            return dsqlUtilities.generateDbConnectAuthToken(tokenGenerator);
        }
    }
}
