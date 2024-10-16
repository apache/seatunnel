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

package org.apache.seatunnel.connectors.tencent.vectordb.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import com.google.common.collect.Lists;
import com.tencent.tcvectordb.client.RPCVectorDBClient;
import com.tencent.tcvectordb.client.VectorDBClient;
import com.tencent.tcvectordb.model.Collection;
import com.tencent.tcvectordb.model.Database;
import com.tencent.tcvectordb.model.param.collection.IndexField;
import com.tencent.tcvectordb.model.param.database.ConnectParam;
import com.tencent.tcvectordb.model.param.enums.ReadConsistencyEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.JSON_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.API_KEY;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.COLLECTION;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.DATABASE;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.URL;
import static org.apache.seatunnel.connectors.tencent.vectordb.config.TencentVectorDBSourceConfig.USER_NAME;

public class ConnectorUtils {
    private ReadonlyConfig config;
    Map<TablePath, CatalogTable> sourceTables;

    public ConnectorUtils(ReadonlyConfig config) {
        this.config = config;
        this.sourceTables = new HashMap<>();
    }

    public Map<TablePath, CatalogTable> getSourceTables() {
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withUrl(config.get(URL))
                        .withUsername(config.get(USER_NAME))
                        .withKey(config.get(API_KEY))
                        .withTimeout(30)
                        .build();
        VectorDBClient client =
                new RPCVectorDBClient(connectParam, ReadConsistencyEnum.EVENTUAL_CONSISTENCY);
        Database database = client.database(config.get(DATABASE));
        Collection collection = database.describeCollection(config.get(COLLECTION));
        TablePath tablePath = TablePath.of(config.get(DATABASE), config.get(COLLECTION));

        List<Column> columns = new ArrayList<>();
        String primaryKey = "id";
        for (IndexField indexField : collection.getIndexes()) {
            if (indexField.isPrimaryKey()) {
                columns.add(
                        PhysicalColumn.builder()
                                .name(indexField.getFieldName())
                                .dataType(STRING_TYPE)
                                .build());
                primaryKey = indexField.getFieldName();
            } else if (indexField.isVectorField()) {
                columns.add(
                        PhysicalColumn.builder()
                                .name(indexField.getFieldName())
                                .dataType(VECTOR_FLOAT_TYPE)
                                .scale(indexField.getDimension())
                                .build());
            }
        }
        Map<String, Object> options = new HashMap<>();
        options.put("isDynamicField", true);
        PhysicalColumn dynamicColumn =
                PhysicalColumn.builder().name("meta").dataType(JSON_TYPE).options(options).build();
        columns.add(dynamicColumn);

        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of(primaryKey, Lists.newArrayList(primaryKey)))
                        .columns(columns)
                        .build();
        Map<TablePath, CatalogTable> sourceTables = new HashMap<>();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("tencent", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");
        sourceTables.put(tablePath, catalogTable);
        return sourceTables;
    }
}
