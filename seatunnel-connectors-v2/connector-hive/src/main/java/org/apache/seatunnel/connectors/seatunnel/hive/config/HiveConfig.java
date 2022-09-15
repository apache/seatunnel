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

package org.apache.seatunnel.connectors.seatunnel.hive.config;

import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveConfig {
    public static final String TABLE_NAME = "table_name";
    public static final String METASTORE_URI = "metastore_uri";
    public static final String TEXT_INPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextInputFormat";
    public static final String TEXT_OUTPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    public static final String PARQUET_INPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String PARQUET_OUTPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    public static final String ORC_INPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    public static final String ORC_OUTPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

    public static Pair<String[], Table> getTableInfo(Config config) {
        String table = config.getString(TABLE_NAME);
        String[] splits = table.split("\\.");
        if (splits.length != 2) {
            throw new RuntimeException("Please config " + TABLE_NAME + " as db.table format");
        }
        HiveMetaStoreProxy hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(config);
        Table tableInformation = hiveMetaStoreProxy.getTable(splits[0], splits[1]);
        return Pair.of(splits, tableInformation);
    }
}
