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

package org.apache.seatunnel.connectors.seatunnel.phoenix.client;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PhoenixOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixOutputFormat.class);

    protected final PhoenixJdbcConnectionProvider connectionProvider;

    private final PhoenixSinkConfig sinkConfig;

    private final transient PhoenixStatementExecutor phoenixWriteExecutor;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public PhoenixOutputFormat(
        PhoenixJdbcConnectionProvider connectionProvider,
        PhoenixSinkConfig phoenixWriteConfig,
        PhoenixStatementExecutor phoenixWriteExecutor) {
        this.connectionProvider = checkNotNull(connectionProvider);
        this.sinkConfig = checkNotNull(phoenixWriteConfig);
        this.phoenixWriteExecutor = checkNotNull(phoenixWriteExecutor);
    }

    public void open()
        throws IOException {
        try {
            connectionProvider.getOrEstablishConnection();
            phoenixWriteExecutor.prepareStatements(connectionProvider.getConnection());
        }
        catch (Exception e) {
            throw new IOException("unable to open JDBC writer", e);
        }

        this.scheduler =
                Executors.newScheduledThreadPool(
                    1, runnable -> {
                        AtomicInteger cnt = new AtomicInteger(0);
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        thread.setName("jdbc-upsert-output-format" + "-" + cnt.incrementAndGet());
                        return thread;
                    });
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                    () -> {
                        synchronized (PhoenixOutputFormat.this) {
                            if (!closed) {
                                try {
                                    flush();
                                }
                                catch (Exception e) {
                                    flushException = e;
                                }
                            }
                        }
                    },
                        sinkConfig.getBatchIntervalMs(),
                        sinkConfig.getBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);

    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", flushException);
        }
    }

    public final synchronized void writeRecord(SeaTunnelRow record)
        throws IOException {
        checkFlushException();
        try {
            addToBatch(record);
            batchCount++;
            if (sinkConfig.getBatchSize() > 0
                && batchCount >= sinkConfig.getBatchSize()) {
                flush();
            }
        }
        catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    protected void addToBatch(SeaTunnelRow record) {
        phoenixWriteExecutor.addToBatch(record);
    }

    public synchronized void flush()
        throws IOException {
        checkFlushException();
        final int sleepMs = 1000;
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                break;
            }
            catch (SQLException e) {
                LOG.error("JDBC executeBatch error, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new IOException("Writing records to Phoenix failed.", e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                }
                catch (Exception exception) {
                    LOG.error(
                        "JDBC connection is not valid, and reestablish connection failed.",
                        exception);
                    throw new IOException("Reestablish JDBC connection failed", exception);
                }

                try {
                    long backoff = Math.min(sinkConfig.getRetryBackoffMultiplierMs() * i,
                            sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush; interrupted while doing another attempt.", e);
                }
            }
        }
    }

    protected void attemptFlush()
        throws SQLException {
        phoenixWriteExecutor.executeBatch();
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     */
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                }
                catch (Exception e) {
                    LOG.warn("Writing records to JDBC failed.", e);
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }

            try {
                if (phoenixWriteExecutor != null) {
                    phoenixWriteExecutor.closeStatements();
                }
            }
            catch (SQLException e) {
                LOG.warn("Close JDBC writer failed.", e);
            }
        }
        connectionProvider.closeConnection();
        checkFlushException();
    }

    public void updateExecutor(boolean reconnect)
        throws SQLException, ClassNotFoundException {
        phoenixWriteExecutor.closeStatements();
        phoenixWriteExecutor.prepareStatements(
            reconnect ? connectionProvider.reestablishConnection() : connectionProvider.getConnection());
    }

    @VisibleForTesting
    public Connection getConnection() {
        return connectionProvider.getConnection();
    }
}
