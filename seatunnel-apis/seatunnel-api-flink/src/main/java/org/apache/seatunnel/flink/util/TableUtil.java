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

package org.apache.seatunnel.flink.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class TableUtil {

    public static DataStream<RowData> tableToDataStream(StreamTableEnvironment tableEnvironment, Table table, boolean isAppend) {
        // change table to stream
        DataType[] tableDataTypes = table.getResolvedSchema().getColumnDataTypes().toArray(new DataType[0]);
        String[] tableFieldNames = table.getResolvedSchema().getColumnNames().toArray(new String[0]);
        TypeInformation<RowData> typeInformation =
                TableUtil.getTypeInformation(tableDataTypes, tableFieldNames);
        if (isAppend) {
            return tableEnvironment.toAppendStream(table, typeInformation);
        } else {
            return tableEnvironment.toRetractStream(table, typeInformation).map(f -> f.f1);
        }
    }


    /**
     * 获取TypeInformation
     *
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static TypeInformation<RowData> getTypeInformation(
            DataType[] dataTypes, String[] fieldNames) {
        return InternalTypeInfo.of(getRowType(dataTypes, fieldNames));
    }


    /**
     * 获取RowType
     *
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static RowType getRowType(DataType[] dataTypes, String[] fieldNames) {
        return RowType.of(
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
                fieldNames);
    }

    public static void dataStreamToTable(StreamTableEnvironment tableEnvironment, String tableName, DataStream<Row> dataStream) {
        tableEnvironment.registerDataStream(tableName, dataStream);
    }

    public static void dataSetToTable(BatchTableEnvironment tableEnvironment, String tableName, DataSet<Row> dataSet) {
        tableEnvironment.registerDataSet(tableName, dataSet);
    }

    public static boolean tableExists(TableEnvironment tableEnvironment, String name) {
        String currentCatalog = tableEnvironment.getCurrentCatalog();
        Catalog catalog = tableEnvironment.getCatalog(currentCatalog).get();
        ObjectPath objectPath = new ObjectPath(tableEnvironment.getCurrentDatabase(), name);
        return catalog.tableExists(objectPath);
    }
}
