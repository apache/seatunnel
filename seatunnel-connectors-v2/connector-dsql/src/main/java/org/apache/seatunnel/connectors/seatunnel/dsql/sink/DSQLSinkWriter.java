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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.dsql.config.DSQLSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class DSQLSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(DSQLSinkWriter.class);

    private final DSQLSinkConfig config;
    private final CatalogTable catalogTable;
    private final DSQLClient dsqlClient;
    private final List<SeaTunnelRow> batchBuffer;
    private final AtomicLong totalWritten = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private long lastLogTime = System.currentTimeMillis();
    private long lastCommitTime = System.currentTimeMillis();
    private static final long LOG_INTERVAL_MS = 60000; // Log stats every minute
    private static final long COMMIT_INTERVAL_MS = 1000;
    private final boolean isMultiTableMode;
    private final String targetTableName;

    public DSQLSinkWriter(DSQLSinkConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.batchBuffer = new ArrayList<>(config.getBatchSize());
        this.isMultiTableMode = config.isEnableMultiTable();
        if (isMultiTableMode) {
            String sourceTable = catalogTable.getTableId().getTableName();
            String database = catalogTable.getTableId().getDatabaseName();
            String sourceTableName = database + "." + sourceTable;
            Map<String, String> tableMapping = config.getTableMapping();
            this.targetTableName = tableMapping.get(sourceTableName);
        } else {
            this.targetTableName = catalogTable.getTableId().getTableName();
        }

        try {
            if (this.targetTableName == null) {
                throw new RuntimeException("mapped table is null");
            }
            // Initialize DSQL client
            this.dsqlClient = new DSQLClient(config, this.targetTableName, catalogTable);
            this.dsqlClient.createTableIfNotExists();
            // Initialize table if needed
            LOG.info(
                    "Initializing DSQL sink writer for table {}.{}",
                    config.getDatabaseName(),
                    this.targetTableName);
        } catch (Exception e) {
            LOG.error("Failed to initialize DSQL sink writer", e);
            throw new RuntimeException("Failed to initialize DSQL sink writer", e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (element == null) {
            return;
        }
        String catalogTableName =
                catalogTable.getTableId().getDatabaseName()
                        + "."
                        + catalogTable.getTableId().getTableName();
        if (!element.getTableId().equals(catalogTableName)) {
            return;
        }

        // Create a copy to avoid potential mutation
        batchBuffer.add(element.copy());
        long currentTime = System.currentTimeMillis();
        if (batchBuffer.size() >= config.getBatchSize()
                || (batchBuffer.size() > 0 && currentTime - lastCommitTime > COMMIT_INTERVAL_MS)) {
            flush();
            logStats();
            lastCommitTime = currentTime;
        }
    }

    private void flush() throws IOException {
        if (batchBuffer.isEmpty()) {
            return;
        }

        int batchSize = batchBuffer.size();
        try {
            LOG.debug("Flushing {} rows to DSQL", batchSize);
            long startTime = System.currentTimeMillis();
            dsqlClient.batchInsert(new ArrayList<>(batchBuffer));
            long endTime = System.currentTimeMillis();

            totalWritten.addAndGet(batchSize);
            batchBuffer.clear();

            LOG.debug(
                    "Successfully flushed {} rows to DSQL in {}ms",
                    batchSize,
                    (endTime - startTime));
        } catch (Exception e) {
            totalFailed.addAndGet(batchSize);
            LOG.error("Failed to flush batch to DSQL: {}", e.getMessage(), e);
            throw new IOException("Failed to write batch to DSQL", e);
        }
    }

    private void logStats() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime > LOG_INTERVAL_MS) {
            LOG.info(
                    "DSQL sink writer stats - Written: {}, Failed: {}",
                    totalWritten.get(),
                    totalFailed.get());
            lastLogTime = currentTime;
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        // Flush any remaining data before commit
        try {
            flush();
            LOG.info(
                    "Preparing commit - Total written: {}, Total failed: {}",
                    totalWritten.get(),
                    totalFailed.get());
        } catch (IOException e) {
            LOG.error("Failed to flush during commit preparation", e);
            throw new RuntimeException("Failed to prepare commit", e);
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        LOG.info(
                "Closing DSQL sink writer - Final stats: Written: {}, Failed: {}",
                totalWritten.get(),
                totalFailed.get());
        try {
            // Flush any remaining data before closing
            if (!batchBuffer.isEmpty()) {
                LOG.info("Flushing remaining {} rows before closing", batchBuffer.size());
                flush();
            }
        } finally {
            try {
                dsqlClient.close();
                LOG.info("DSQL client closed successfully");
            } catch (Exception e) {
                LOG.warn("Error closing DSQL client: {}", e.getMessage(), e);
            }
        }
    }
}
