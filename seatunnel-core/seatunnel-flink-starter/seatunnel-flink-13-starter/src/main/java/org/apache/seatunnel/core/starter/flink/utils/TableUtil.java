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

package org.apache.seatunnel.core.starter.flink.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public final class TableUtil {

    private TableUtil() {}

    public static DataStream<Row> tableToDataStream(
            StreamTableEnvironment tableEnvironment, Table table, boolean isAppend) {

        TypeInformation<Row> typeInfo = table.getSchema().toRowType();
        if (isAppend) {
            return tableEnvironment.toAppendStream(table, typeInfo);
        }
        return tableEnvironment
                .toRetractStream(table, typeInfo)
                .filter(row -> row.f0)
                .map(row -> row.f1)
                .returns(typeInfo);
    }

    public static boolean tableExists(TableEnvironment tableEnvironment, String name) {
        return Arrays.asList(tableEnvironment.listTables()).contains(name);
    }
}
