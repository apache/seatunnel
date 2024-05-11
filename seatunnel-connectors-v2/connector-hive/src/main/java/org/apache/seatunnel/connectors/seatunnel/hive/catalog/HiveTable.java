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

package org.apache.seatunnel.connectors.seatunnel.hive.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class HiveTable {

    private CatalogTable catalogTable;
    private Map<String, String> tableParameters;
    private FileFormat inputFormat;
    private String location;

    public static HiveTable of(
            CatalogTable catalogTable,
            Map<String, String> tableParameters,
            String inputFormat,
            String location) {
        return new HiveTable(catalogTable, tableParameters, parseFormat(inputFormat), location);
    }

    private static FileFormat parseFormat(String inputFormat) {
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
