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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceOptions;

import org.apache.hadoop.hive.metastore.api.Table;

public class HiveTableUtils {

    public static Table getTableInfo(ReadonlyConfig readonlyConfig) {
        String table = readonlyConfig.get(HiveSourceOptions.TABLE_NAME);
        TablePath tablePath = TablePath.of(table);
        if (tablePath.getDatabaseName() == null || tablePath.getTableName() == null) {
            throw new SeaTunnelRuntimeException(
                    HiveConnectorErrorCode.HIVE_TABLE_NAME_ERROR, "Current table name is " + table);
        }
        HiveMetaStoreProxy hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(readonlyConfig);
        try {
            return hiveMetaStoreProxy.getTable(
                    tablePath.getDatabaseName(), tablePath.getTableName());
        } finally {
            hiveMetaStoreProxy.close();
        }
    }

    public static FileFormat parseFileFormat(Table table) {
        String inputFormat = table.getSd().getInputFormat();
        if (HiveConstants.TEXT_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.TEXT;
        }
        if (HiveConstants.PARQUET_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.PARQUET;
        }
        if (HiveConstants.ORC_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.ORC;
        }
        throw new HiveConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Hive connector only support [text parquet orc] table now");
    }
}
