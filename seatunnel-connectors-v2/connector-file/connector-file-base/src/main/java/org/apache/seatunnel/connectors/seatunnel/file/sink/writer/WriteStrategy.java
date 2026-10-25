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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

public interface WriteStrategy<T> extends Transaction, Serializable, Closeable {
    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex);

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param conf hadoop conf
     * @return Configuration
     */
    Configuration getConfiguration(HadoopConf conf);

    /**
     * write seaTunnelRow to target datasource
     *
     * @param seaTunnelRow seaTunnelRow
     * @throws FileConnectorException Exceptions
     */
    void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException;

    /**
     * set catalog table to write strategy
     *
     * @param catalogTable catalogTable
     */
    void setCatalogTable(CatalogTable catalogTable);

    /**
     * use seaTunnelRow generate partition directory
     *
     * @param seaTunnelRow seaTunnelRow
     * @return the map of partition directory
     */
    LinkedHashMap<String, List<String>> generatorPartitionDir(SeaTunnelRow seaTunnelRow);

    T getOrCreateOutputStream(String path) throws IOException;

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    String generateFileName(String transactionId);

    /** when a transaction is triggered, release resources */
    void finishAndCloseFile();

    /**
     * get current checkpoint id
     *
     * @return checkpoint id
     */
    long getCheckpointId();

    /**
     * get sink configuration
     *
     * @return sink configuration
     */
    FileSinkConfig getFileSinkConfig();

    /**
     * get file system utils
     *
     * @return file system utils
     */
    HadoopFileSystemProxy getHadoopFileSystemProxy();

    /**
     * Apply a schema change event to this write strategy. Implementations must: 1. Close and flush
     * all currently open writers (resource-safe: attempt all closes) 2. Update the row type and
     * column index mapping 3. Invalidate any cached format-specific schema objects
     *
     * <p>The next {@link #write(SeaTunnelRow)} call after this method returns will open new writers
     * with the updated schema.
     *
     * <p>This method is a no-op when {@code schema_evolution_enabled=false}.
     *
     * @param event the schema change event (ADD/DROP/RENAME/UPDATE column, or batch)
     * @throws IOException if closing any open writer fails
     */
    void applySchemaChange(SchemaChangeEvent event) throws IOException;
}
