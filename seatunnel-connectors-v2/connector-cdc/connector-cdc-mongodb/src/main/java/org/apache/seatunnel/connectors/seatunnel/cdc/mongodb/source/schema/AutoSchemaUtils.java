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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions;

import org.apache.commons.collections4.CollectionUtils;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@Slf4j
public class AutoSchemaUtils {

    public static String CATALOG_NAME = "MongoDB";

    public static List<CatalogTable> autoSchemaFromConfig(ReadonlyConfig config) {
        final String hosts = config.get(MongodbSourceOptions.HOSTS);
        final List<String> databaseList = config.get(MongodbSourceOptions.DATABASE);
        if (CollectionUtils.isNotEmpty(databaseList)) {
            throw new SeaTunnelException("database must not is empty");
        }
        final List<String> collectionNameList = config.get(MongodbSourceOptions.COLLECTION);
        if (CollectionUtils.isNotEmpty(collectionNameList)) {
            throw new SeaTunnelException("collection must not is empty");
        }
        String url = String.format("mongodb://%s", hosts);
        try (MongoClient mongoClient = MongoClients.create(url)) {
            List<CatalogTable> catalogTableList = new ArrayList<>();
            collectionNameList.forEach(
                    collectionName -> {
                        final String[] split = collectionName.split("\\.");
                        if (split.length != 2) {
                            throw new SeaTunnelException(
                                    String.format("Invalid collection name %s", collectionName));
                        }
                        String currentDatabase = split[0];
                        String currentCollection = split[1];
                        catalogTableList.add(
                                getCatalogTable(mongoClient, currentDatabase, currentCollection));
                    });
            return catalogTableList;
        }
    }

    private static CatalogTable getCatalogTable(
            MongoClient mongoClient, String databaseName, String collectionName) {
        MongoCollection<BsonDocument> collection =
                mongoClient
                        .getDatabase(databaseName)
                        .getCollection(collectionName, BsonDocument.class);
        final BsonDocument first = collection.find().first();
        if (first == null) {
            throw new SeaTunnelException(
                    "When turning on automatic schema generation, make sure there is at least one piece of data in the collection.");
        }
        final List<Column> columnList = getColumns(first);
        return CatalogTable.of(
                tableIdentifierOf(databaseName, collectionName),
                tableSchemaOf(columnList),
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }

    private static List<Column> getColumns(BsonDocument first) {
        final Set<String> fieldNameSet = first.keySet();
        List<Column> columnList = new ArrayList<>();
        fieldNameSet.forEach(
                fieldName -> {
                    final BsonValue bsonValue = first.get(fieldName);
                    SeaTunnelDataType<?> dataType =
                            MongodbValueToTypeConvertor.convertTypeFromValue(bsonValue);
                    PhysicalColumn column =
                            PhysicalColumn.of(fieldName, dataType, null, null, true, null, null);
                    columnList.add(column);
                });
        return columnList;
    }

    private static TableSchema tableSchemaOf(List<Column> columnList) {
        return new TableSchema(columnList, null, new ArrayList<>());
    }

    private static TableIdentifier tableIdentifierOf(String databaseName, String collectionName) {
        return TableIdentifier.of(CATALOG_NAME, databaseName, collectionName);
    }
}
