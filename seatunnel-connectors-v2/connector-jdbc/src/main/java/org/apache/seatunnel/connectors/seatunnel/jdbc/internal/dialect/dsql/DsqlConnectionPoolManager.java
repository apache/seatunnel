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

import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;
import software.amazon.awssdk.services.dsql.model.GenerateAuthTokenRequest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
public class DsqlConnectionPoolManager {

    private HikariDataSource connectionPool;
    private Map<Integer, Connection> connectionMap;
    private AwsCredentialsProvider provider;
    private DsqlUtilities dsqlUtilities;
    private JdbcConnectionConfig jdbcConfig;
    private ScheduledExecutorService tokenRefreshExecutor;

    DsqlConnectionPoolManager(JdbcConnectionConfig jdbcConfig) {
        initAWSInfo(jdbcConfig);
        this.connectionPool = new HikariDataSource();
        this.connectionPool.setIdleTimeout(30 * 1000);
        this.connectionPool.setMaximumPoolSize(10);
        this.connectionPool.setJdbcUrl(jdbcConfig.getUrl());
        this.connectionPool.setPassword(generateAuthToken(getDBHost()));
        this.connectionPool.setDriverClassName(jdbcConfig.getDriverName());
        this.connectionPool.setUsername(jdbcConfig.getUsername().get());
        this.connectionPool.setAutoCommit(jdbcConfig.isAutoCommit());
        this.connectionMap = new ConcurrentHashMap<>();
        this.tokenRefreshExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "dsql-token-refresh");
                            t.setDaemon(true);
                            return t;
                        });
        // Schedule token refresh every 10 minutes (tokens are valid for 15 minutes)
        tokenRefreshExecutor.scheduleAtFixedRate(this::resetPassword, 10, 10, TimeUnit.MINUTES);
    }

    public void initAWSInfo(JdbcConnectionConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        this.provider =
                new AwsCredentialsProvider() {
                    @Override
                    public AwsCredentials resolveCredentials() {
                        return AwsBasicCredentials.create(
                                jdbcConfig.getAccessKeyId(), jdbcConfig.getSecretAccessKey());
                    }
                };
        this.dsqlUtilities =
                this.dsqlUtilities =
                        DsqlUtilities.builder()
                                .region(Region.of(jdbcConfig.getRegion()))
                                .credentialsProvider(provider)
                                .build();
    }

    private void resetPassword() {
        connectionPool.getHikariConfigMXBean().setPassword(generateAuthToken(getDBHost()));
        log.warn("Reset password for dsql connection successfully!");
    }

    private String getDBHost() {
        String url = jdbcConfig.getUrl();
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(url);
        return urlInfo.getHost();
    }

    private String generateAuthToken(String clusterEndpoint) {

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

    public Connection getConnection(int index) {
        return connectionMap.computeIfAbsent(
                index,
                i -> {
                    try {
                        return connectionPool.getConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public boolean containsConnection(int index) {
        return connectionMap.containsKey(index);
    }

    public Connection remove(int index) {
        return connectionMap.remove(index);
    }

    public String getPoolName() {
        return connectionPool.getPoolName();
    }

    public void close() {
        if (!connectionPool.isClosed()) {
            connectionPool.close();
        }
        if (!tokenRefreshExecutor.isShutdown()) {
            tokenRefreshExecutor.shutdownNow();
        }
    }
}
