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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single streaming split used by the Vitess connector.
 *
 * <p>The split persists both the last committed Debezium source offset and the latest Debezium
 * table definitions needed to decode backlog rows after checkpoint restore.
 */
public class VitessSourceSplit implements SourceSplit {

    public static final String SPLIT_ID = "vitess-stream-split";

    private static final long serialVersionUID = 1L;

    /** Stable split id because the connector currently owns one streaming split per source. */
    private final String splitId;

    /** Debezium source offset stored in SeaTunnel checkpoints. */
    private Map<String, Object> offset;

    /**
     * Serialized Debezium table definitions captured from FIELD events.
     *
     * <p>Vitess does not guarantee that field metadata is replayed after every resume position, so
     * restore must keep the latest known schema together with the VGTID.
     */
    private Map<String, byte[]> tableSchemas;

    /**
     * SeaTunnel-side table schemas used to rebuild row converters during checkpoint restore.
     *
     * <p>The first Vitess delivery keeps schema evolution out of scope, so this stays aligned with
     * the declared table schemas and must survive restart unchanged.
     */
    private List<CatalogTable> checkpointTables;

    public VitessSourceSplit(String splitId, Map<String, Object> offset) {
        this(splitId, offset, null, null);
    }

    public VitessSourceSplit(
            String splitId, Map<String, Object> offset, Map<String, byte[]> tableSchemas) {
        this(splitId, offset, tableSchemas, null);
    }

    public VitessSourceSplit(
            String splitId,
            Map<String, Object> offset,
            Map<String, byte[]> tableSchemas,
            List<CatalogTable> checkpointTables) {
        this.splitId = splitId;
        this.offset = copyOffset(offset);
        this.tableSchemas = copyTableSchemas(tableSchemas);
        this.checkpointTables = copyCheckpointTables(checkpointTables);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    /**
     * Returns a defensive copy so checkpoint callers cannot mutate in-memory state accidentally.
     */
    public Map<String, Object> getOffset() {
        return copyOffset(offset);
    }

    /** Updates the in-memory checkpoint position after a record has been emitted successfully. */
    public void setOffset(Map<String, ?> offset) {
        this.offset = copyOffset(offset);
    }

    /** Returns the latest serialized Debezium table schemas captured for this split. */
    public Map<String, byte[]> getTableSchemas() {
        return copyTableSchemas(tableSchemas);
    }

    /** Updates the schema snapshot so restore can decode rows before new FIELD events arrive. */
    public void setTableSchemas(Map<String, byte[]> tableSchemas) {
        this.tableSchemas = copyTableSchemas(tableSchemas);
    }

    /** Returns the SeaTunnel schema snapshot used to rebuild the deserializer on restore. */
    public List<CatalogTable> getCheckpointTables() {
        return copyCheckpointTables(checkpointTables);
    }

    /** Creates a deep-enough copy for checkpoint serialization. */
    public VitessSourceSplit copy() {
        return new VitessSourceSplit(splitId, offset, tableSchemas, checkpointTables);
    }

    @Override
    public String toString() {
        return "VitessSourceSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + offset
                + ", tableSchemas="
                + (tableSchemas == null ? 0 : tableSchemas.size())
                + ", checkpointTables="
                + (checkpointTables == null ? 0 : checkpointTables.size())
                + '}';
    }

    private static Map<String, Object> copyOffset(Map<String, ?> offset) {
        if (offset == null || offset.isEmpty()) {
            return null;
        }
        return new HashMap<>(offset);
    }

    private static Map<String, byte[]> copyTableSchemas(Map<String, byte[]> tableSchemas) {
        if (tableSchemas == null || tableSchemas.isEmpty()) {
            return null;
        }
        Map<String, byte[]> copiedSchemas = new HashMap<>(tableSchemas.size());
        tableSchemas.forEach(
                (tableId, schemaBytes) ->
                        copiedSchemas.put(
                                tableId, schemaBytes == null ? null : schemaBytes.clone()));
        return copiedSchemas;
    }

    private static List<CatalogTable> copyCheckpointTables(List<CatalogTable> checkpointTables) {
        if (checkpointTables == null || checkpointTables.isEmpty()) {
            return null;
        }
        return new ArrayList<>(checkpointTables);
    }
}
