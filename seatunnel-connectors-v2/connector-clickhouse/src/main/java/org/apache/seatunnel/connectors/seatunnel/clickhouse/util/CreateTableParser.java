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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * @deprecated Use {@link org.apache.seatunnel.connectors.seatunnel.common.util.CreateTableParser}.
 */
@Deprecated
public class CreateTableParser {

    /** Parses through connector-common while retaining this package's legacy result type. */
    public static Map<String, ColumnInfo> getColumnList(String createTableSql) {
        Map<String, ColumnInfo> columns = new HashMap<>();
        org.apache.seatunnel.connectors.seatunnel.common.util.CreateTableParser.getColumnList(
                        createTableSql)
                .forEach(
                        (name, column) ->
                                columns.put(
                                        name,
                                        new ColumnInfo(
                                                column.getName(),
                                                column.getInfo(),
                                                column.getStartIndex(),
                                                column.getEndIndex())));
        return columns;
    }

    @Getter
    public static final class ColumnInfo {

        public ColumnInfo(String name, String info, int startIndex, int endIndex) {
            this.name = name;
            this.info = info;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        String name;
        String info;
        int startIndex;
        int endIndex;
    }
}
