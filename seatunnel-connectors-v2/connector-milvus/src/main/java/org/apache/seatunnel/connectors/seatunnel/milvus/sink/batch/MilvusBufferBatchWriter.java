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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.batch;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.milvus.convert.MilvusConvertUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;

import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;

public class MilvusBufferBatchWriter implements MilvusBatchWriter {

    private final int batchSize;
    private final CatalogTable catalogTable;
    private final Boolean autoId;
    private final Boolean enableUpsert;
    private final String collectionName;
    private MilvusClientV2 milvusClient;

    private volatile List<JsonObject> milvusDataCache;
    private volatile int writeCount = 0;
    private static final Gson GSON = new Gson();

    public MilvusBufferBatchWriter(
            CatalogTable catalogTable,
            Integer batchSize,
            Boolean autoId,
            Boolean enableUpsert,
            MilvusClientV2 milvusClient) {
        this.catalogTable = catalogTable;
        this.autoId = autoId;
        this.enableUpsert = enableUpsert;
        this.milvusClient = milvusClient;
        this.collectionName = catalogTable.getTablePath().getTableName();
        this.batchSize = batchSize;
        this.milvusDataCache = new ArrayList<>(batchSize);
    }

    @Override
    public void addToBatch(SeaTunnelRow element) {
        JsonObject data = buildMilvusData(element);
        milvusDataCache.add(data);
        writeCount++;
    }

    @Override
    public boolean needFlush() {
        return this.writeCount >= this.batchSize;
    }

    @Override
    public synchronized boolean flush() {
        if (CollectionUtils.isEmpty(this.milvusDataCache)) {
            return true;
        }
        writeData2Collection();
        this.milvusDataCache = new ArrayList<>(this.batchSize);
        this.writeCount = 0;
        return true;
    }

    @Override
    public void close() {
        try {
            this.milvusClient.close(10);
        } catch (InterruptedException e) {
            throw new SeaTunnelException(e);
        }
    }

    private JsonObject buildMilvusData(SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();

        JsonObject data = new JsonObject();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String fieldName = seaTunnelRowType.getFieldNames()[i];

            if (autoId && isPrimaryKeyField(primaryKey, fieldName)) {
                continue; // if create table open AutoId, then don't need insert data with
                // primaryKey field.
            }

            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            Object value = element.getField(i);
            if (null == value) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.FIELD_IS_NULL, fieldName);
            }

            data.add(
                    fieldName,
                    GSON.toJsonTree(MilvusConvertUtils.convertBySeaTunnelType(fieldType, value)));
        }
        return data;
    }

    private void writeData2Collection() {
        // default to use upsertReq, but upsert only works when autoID is disabled
        if (enableUpsert && !autoId) {
            UpsertReq upsertReq =
                    UpsertReq.builder()
                            .collectionName(this.collectionName)
                            .data(this.milvusDataCache)
                            .build();
            milvusClient.upsert(upsertReq);
        } else {
            InsertReq insertReq =
                    InsertReq.builder()
                            .collectionName(this.collectionName)
                            .data(this.milvusDataCache)
                            .build();
            milvusClient.insert(insertReq);
        }
    }
}
