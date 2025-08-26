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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface ReadStrategy extends Serializable, Closeable {
    void init(HadoopConf conf);

    void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException;

    /**
     * Read data from a file split (partial file with offset and length)
     *
     * @param filePath the file path
     * @param tableId the table identifier
     * @param output the output collector
     * @param startOffset the start byte offset in the file
     * @param length the length to read (-1 means read to end)
     * @param isFirstSplit whether this is the first split (affects header handling)
     * @throws IOException if file operations fail
     * @throws FileConnectorException if file connector specific errors occur
     */
    default void readSplit(
            String filePath,
            String tableId,
            Collector<SeaTunnelRow> output,
            long startOffset,
            long length,
            boolean isFirstSplit)
            throws IOException, FileConnectorException {
        // Default implementation falls back to reading the entire file
        read(filePath, tableId, output);
    }

    SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException;

    default SeaTunnelRowType getSeaTunnelRowTypeInfo(TablePath tablePath, String path)
            throws FileConnectorException {
        return getSeaTunnelRowTypeInfo(path);
    }

    default SeaTunnelRowType getSeaTunnelRowTypeInfoWithUserConfigRowType(
            String path, SeaTunnelRowType rowType) throws FileConnectorException {
        return getSeaTunnelRowTypeInfo(path);
    }

    void setCatalogTable(CatalogTable catalogTable);

    List<String> getFileNamesByPath(String path) throws IOException;

    // todo: use ReadonlyConfig
    void setPluginConfig(Config pluginConfig);

    // todo: use CatalogTable
    SeaTunnelRowType getActualSeaTunnelRowTypeInfo();

    default <T> void buildColumnsWithErrorCheck(
            TablePath tablePath, Iterator<T> keys, Consumer<T> getDataType) {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (keys.hasNext()) {
            try {
                getDataType.accept(keys.next());
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    this.getClass().getSimpleName().replace("ReadStrategy", ""),
                    tablePath.getFullName(),
                    unsupported);
        }
    }
}
