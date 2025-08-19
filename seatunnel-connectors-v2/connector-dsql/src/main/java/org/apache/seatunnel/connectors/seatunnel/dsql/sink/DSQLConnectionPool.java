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

package org.apache.seatunnel.connectors.seatunnel.dsql.sink;

import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariConfig;
import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

import org.apache.seatunnel.connectors.seatunnel.dsql.config.DSQLSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;
import software.amazon.awssdk.services.dsql.model.GenerateAuthTokenRequest;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Connection pool manager for DSQL with token refresh capability */
public class DSQLConnectionPool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DSQLConnectionPool.class);
    private static final ConcurrentHashMap<String, DSQLConnectionPool> POOL_CACHE =
            new ConcurrentHashMap<>();

    private final DSQLSinkConfig config;
    private final DsqlUtilities dsqlUtilities;
    private final String hostname;
    private final HikariDataSource dataSource;
    private final ScheduledExecutorService tokenRefreshExecutor;
    private volatile String currentToken;

    private DSQLConnectionPool(DSQLSinkConfig config, DsqlUtilities dsqlUtilities) {
        this.config = config;
        this.dsqlUtilities = dsqlUtilities;
        this.hostname = extractHostname(config.getClusterEndpoint());
        this.tokenRefreshExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "dsql-token-refresh");
                            t.setDaemon(true);
                            return t;
                        });

        // Generate initial token
        this.currentToken = generateAuthToken();

        // Create HikariCP data source
        this.dataSource = createDataSource();

        // Schedule token refresh every 10 minutes (tokens are valid for 15 minutes)
        tokenRefreshExecutor.scheduleAtFixedRate(this::refreshToken, 10, 10, TimeUnit.MINUTES);

        LOG.info("DSQL connection pool initialized for endpoint: {}", hostname);
    }

    public static DSQLConnectionPool getInstance(
            DSQLSinkConfig config, DsqlUtilities dsqlUtilities) {
        String key = config.getClusterEndpoint() + ":" + config.getDatabaseName();
        return POOL_CACHE.computeIfAbsent(key, k -> new DSQLConnectionPool(config, dsqlUtilities));
    }

    private HikariDataSource createDataSource() {
        HikariConfig hikariConfig = new HikariConfig();

        // Basic connection settings
        hikariConfig.setJdbcUrl(
                "jdbc:postgresql://" + hostname + ":5432/" + config.getDatabaseName());
        hikariConfig.setUsername(config.getUserName());
        hikariConfig.setPassword(currentToken);

        // SSL settings
        hikariConfig.addDataSourceProperty("sslMode", "verify-full");
        hikariConfig.addDataSourceProperty(
                "sslFactory", "org.postgresql.ssl.DefaultJavaSSLFactory");

        // Pool settings
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(config.getConnectionTimeoutMs());
        hikariConfig.setIdleTimeout(600000); // 10 minutes
        hikariConfig.setMaxLifetime(900000); // 15 minutes (token validity)
        hikariConfig.setLeakDetectionThreshold(60000); // 1 minute

        // Pool name
        hikariConfig.setPoolName("DSQL-Pool-" + config.getDatabaseName());

        return new HikariDataSource(hikariConfig);
    }

    private String extractHostname(String clusterEndpoint) {
        if (clusterEndpoint.startsWith("arn:aws:dsql:")) {
            String[] parts = clusterEndpoint.split(":");
            if (parts.length >= 6) {
                String clusterId = parts[5].replace("cluster/", "");
                return clusterId + ".dsql." + parts[3] + ".on.aws";
            }
        }
        return clusterEndpoint;
    }

    private String generateAuthToken() {
        GenerateAuthTokenRequest tokenGenerator =
                GenerateAuthTokenRequest.builder()
                        .hostname(hostname)
                        .region(Region.of(config.getAwsRegion()))
                        .build();

        if ("admin".equals(config.getUserName())) {
            return dsqlUtilities.generateDbConnectAdminAuthToken(tokenGenerator);
        } else {
            return dsqlUtilities.generateDbConnectAuthToken(tokenGenerator);
        }
    }

    private void refreshToken() {
        try {
            String newToken = generateAuthToken();
            this.currentToken = newToken;

            // Update the data source password
            dataSource.getHikariConfigMXBean().setPassword(newToken);

            LOG.debug("DSQL authentication token refreshed successfully");
        } catch (Exception e) {
            LOG.error("Failed to refresh DSQL authentication token", e);
        }
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        try {
            tokenRefreshExecutor.shutdown();
            if (!tokenRefreshExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                tokenRefreshExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            tokenRefreshExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }

        LOG.info("DSQL connection pool closed");
    }

    public static void closeAll() {
        POOL_CACHE
                .values()
                .forEach(
                        pool -> {
                            try {
                                pool.close();
                            } catch (Exception e) {
                                LOG.warn("Error closing connection pool", e);
                            }
                        });
        POOL_CACHE.clear();
    }
}
