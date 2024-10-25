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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.table.catalog.TablePath;

import java.util.Arrays;
import java.util.List;

public class MetadataUtil {

    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String PARTITION = "partition";
    public static final String ROW_KIND = "rowKind";
    public static final String EVENT_TIME = "ts_ms";
    public static final String DELAY = "delay";

    public static final List<String> METADATA_FIELDS =
            Arrays.asList(DELAY, PARTITION, DATABASE, TABLE, ROW_KIND, EVENT_TIME);

    public static void setDelay(SeaTunnelRow row, Long delay) {
        row.getMetadata().put(DELAY, delay);
    }

    public static void setPartition(SeaTunnelRow row, String[] partition) {
        row.getMetadata().put(PARTITION, partition);
    }

    public static void setEventTime(SeaTunnelRow row, Long delay) {
        row.getMetadata().put(EVENT_TIME, delay);
    }

    public static Long getDelay(SeaTunnelRow row) {
        return (Long) row.getMetadata().get(DELAY);
    }

    public static String getDatabase(SeaTunnelRow row) {
        TablePath tablePath = TablePath.of(row.getTableId());
        return tablePath.getDatabaseName();
    }

    public static String getTable(SeaTunnelRow row) {
        TablePath tablePath = TablePath.of(row.getTableId());
        return tablePath.getTableName();
    }

    public static RowKind getRowKind(SeaTunnelRow row) {
        return row.getRowKind();
    }

    public static String getPartitionStr(SeaTunnelRow row) {
        return String.join(",", (String[]) row.getMetadata().get(PARTITION));
    }

    public static String[] getPartition(SeaTunnelRow row) {
        return (String[]) row.getMetadata().get(PARTITION);
    }

    public static Long getEventTime(SeaTunnelRow row) {
        return (Long) row.getMetadata().get(EVENT_TIME);
    }

    public static boolean isMetadataField(String fieldName) {
        return METADATA_FIELDS.contains(fieldName);
    }
}
