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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.table.type.CommonOptions.DELAY;
import static org.apache.seatunnel.api.table.type.CommonOptions.EVENT_TIME;
import static org.apache.seatunnel.api.table.type.CommonOptions.IS_BINARY_FORMAT;
import static org.apache.seatunnel.api.table.type.CommonOptions.IS_COMPLETE;
import static org.apache.seatunnel.api.table.type.CommonOptions.PARTITION;

public class MetadataUtil {

    public static final List<String> METADATA_FIELDS;

    static {
        METADATA_FIELDS = new ArrayList<>();
        Stream.of(CommonOptions.values())
                .filter(CommonOptions::isSupportMetadataTrans)
                .map(CommonOptions::getName)
                .forEach(METADATA_FIELDS::add);
    }

    public static void setDelay(SeaTunnelRow row, Long delay) {
        row.getOptions().put(DELAY.getName(), delay);
    }

    public static void setPartition(SeaTunnelRow row, String[] partition) {
        row.getOptions().put(PARTITION.getName(), partition);
    }

    public static void setEventTime(SeaTunnelRow row, Long delay) {
        row.getOptions().put(EVENT_TIME.getName(), delay);
    }

    public static void setBinaryRowComplete(SeaTunnelRow row) {
        row.getOptions().put(IS_COMPLETE.getName(), true);
    }

    public static void setBinaryFormat(SeaTunnelRow row) {
        row.getOptions().put(IS_BINARY_FORMAT.getName(), true);
    }

    public static boolean isComplete(Object row) {
        return checkOption(row, IS_COMPLETE.getName(), false);
    }

    public static boolean isBinaryFormat(Object row) {
        return checkOption(row, IS_BINARY_FORMAT.getName(), false);
    }

    public static String getDatabase(SeaTunnelRowAccessor row) {
        if (row.getTableId() == null) {
            return null;
        }
        return TablePath.of(row.getTableId()).getDatabaseName();
    }

    public static String getTable(SeaTunnelRowAccessor row) {
        if (row.getTableId() == null) {
            return null;
        }
        return TablePath.of(row.getTableId()).getTableName();
    }

    public static String getRowKind(SeaTunnelRowAccessor row) {
        return row.getRowKind().shortString();
    }

    public static String[] getPartition(SeaTunnelRowAccessor row) {
        return (String[]) row.getOptions().get(PARTITION.getName());
    }

    public static boolean isMetadataField(String fieldName) {
        return METADATA_FIELDS.contains(fieldName);
    }

    public static <T> boolean checkOption(T row, String optionKey, boolean defaultValue) {
        if (row instanceof SeaTunnelRow) {
            return ((SeaTunnelRow) row)
                    .getOptions()
                    .getOrDefault(optionKey, defaultValue)
                    .equals(true);
        } else if (row instanceof SeaTunnelRowAccessor) {
            return ((SeaTunnelRowAccessor) row)
                    .getOptions()
                    .getOrDefault(optionKey, defaultValue)
                    .equals(true);
        }
        throw new IllegalArgumentException("Unsupported row type: " + row.getClass().getName());
    }
}
