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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.SaveModeConstants;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import java.util.stream.Collectors;

public class StarRocksSaveModeUtil {

    static String fillingCreateSql(String template, String database, String table, TableSchema tableSchema) {
        String primaryKey = tableSchema.getPrimaryKey().getColumnNames().stream().map(r -> "`" + r + "`").collect(Collectors.joining(","));
        String rowTypeFields = "";
        return template.replace(String.format("{%s}", SaveModeConstants.DATABASE), database)
            .replace(String.format("{%s}", SaveModeConstants.TABLE_NAME), table)
            .replace(String.format("{%s}", SaveModeConstants.ROWTYPE_FIELDS), rowTypeFields)
            .replace(String.format("{%s}", SaveModeConstants.ROWTYPE_PRIMARY_KEY), primaryKey);
    }

}
