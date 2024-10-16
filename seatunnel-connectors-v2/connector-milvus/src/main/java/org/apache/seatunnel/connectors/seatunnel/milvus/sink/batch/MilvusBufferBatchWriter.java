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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.utils.MilvusConvertUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AlterCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.CREATE_INDEX;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.ENABLE_AUTO_ID;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.ENABLE_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.LOAD_COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.RATE_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.URL;

@Slf4j
public class MilvusBufferBatchWriter implements MilvusBatchWriter {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig config;
    private final String collectionName;
    private final Boolean autoId;
    private final Boolean enableUpsert;
    private Boolean hasPartitionKey;

    private MilvusClientV2 milvusClient;
    private int batchSize;
    private volatile Map<String, List<JsonObject>> milvusDataCache;
    private final AtomicLong writeCache = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    public MilvusBufferBatchWriter(CatalogTable catalogTable, ReadonlyConfig config)
            throws SeaTunnelException {
        this.catalogTable = catalogTable;
        this.config = config;
        this.autoId =
                getAutoId(
                        catalogTable.getTableSchema().getPrimaryKey(), config.get(ENABLE_AUTO_ID));
        this.enableUpsert = config.get(ENABLE_UPSERT);
        this.batchSize = config.get(BATCH_SIZE);
        this.collectionName = catalogTable.getTablePath().getTableName();
        this.milvusDataCache = new HashMap<>();
        initMilvusClient(config);
    }
    /*
     * set up the Milvus client
     */
    private void initMilvusClient(ReadonlyConfig config) throws SeaTunnelException {
        try {
            log.info("begin to init Milvus client");
            String dbName = catalogTable.getTablePath().getDatabaseName();
            String collectionName = catalogTable.getTablePath().getTableName();

            ConnectConfig connectConfig =
                    ConnectConfig.builder().uri(config.get(URL)).token(config.get(TOKEN)).build();
            this.milvusClient = new MilvusClientV2(connectConfig);
            if (StringUtils.isNotEmpty(dbName)) {
                milvusClient.useDatabase(dbName);
            }
            this.hasPartitionKey =
                    MilvusConnectorUtils.hasPartitionKey(milvusClient, collectionName);
            // set rate limit
            if (config.get(RATE_LIMIT) > 0) {
                log.info("set rate limit for collection: " + collectionName);
                Map<String, String> properties = new HashMap<>();
                properties.put("collection.insertRate.max.mb", config.get(RATE_LIMIT).toString());
                properties.put("collection.upsertRate.max.mb", config.get(RATE_LIMIT).toString());
                AlterCollectionReq alterCollectionReq =
                        AlterCollectionReq.builder()
                                .collectionName(collectionName)
                                .properties(properties)
                                .build();
                milvusClient.alterCollection(alterCollectionReq);
            }
            try {
                if (config.get(CREATE_INDEX)) {
                    // create index
                    log.info("create index for collection: " + collectionName);
                    DescribeCollectionResp describeCollectionResp =
                            milvusClient.describeCollection(
                                    DescribeCollectionReq.builder()
                                            .collectionName(collectionName)
                                            .build());
                    List<IndexParam> indexParams = new ArrayList<>();
                    for (String fieldName : describeCollectionResp.getVectorFieldNames()) {
                        IndexParam indexParam =
                                IndexParam.builder()
                                        .fieldName(fieldName)
                                        .metricType(IndexParam.MetricType.COSINE)
                                        .build();
                        indexParams.add(indexParam);
                    }
                    CreateIndexReq createIndexReq =
                            CreateIndexReq.builder()
                                    .collectionName(collectionName)
                                    .indexParams(indexParams)
                                    .build();
                    milvusClient.createIndex(createIndexReq);
                }
            } catch (Exception e) {
                log.warn("create index failed, maybe index already exists");
            }
            if (config.get(LOAD_COLLECTION)
                    && !milvusClient.getLoadState(
                            GetLoadStateReq.builder().collectionName(collectionName).build())) {
                log.info("load collection: " + collectionName);
                milvusClient.loadCollection(
                        LoadCollectionReq.builder().collectionName(collectionName).build());
            }
            log.info("init Milvus client success");
        } catch (Exception e) {
            log.error("init Milvus client failed", e);
            throw new MilvusConnectorException(MilvusConnectionErrorCode.INIT_CLIENT_ERROR, e);
        }
    }

    private Boolean getAutoId(PrimaryKey primaryKey, Boolean enableAutoId) {
        if (null != primaryKey && null != primaryKey.getEnableAutoId()) {
            return primaryKey.getEnableAutoId();
        } else {
            return enableAutoId;
        }
    }

    @Override
    public void addToBatch(SeaTunnelRow element) {
        // put data to cache by partition
        if (StringUtils.isNotEmpty(element.getPartitionName())
                && !milvusDataCache.containsKey(element.getPartitionName())) {
            String partitionName = element.getPartitionName();
            Boolean partitions =
                    milvusClient.hasPartition(
                            HasPartitionReq.builder()
                                    .collectionName(collectionName)
                                    .partitionName(partitionName)
                                    .build());
            if (!partitions) {
                log.info("create partition: " + partitionName);
                CreatePartitionReq createPartitionReq =
                        CreatePartitionReq.builder()
                                .collectionName(collectionName)
                                .partitionName(partitionName)
                                .build();
                milvusClient.createPartition(createPartitionReq);
                log.info("create partition success");
            }
        }
        JsonObject data = buildMilvusData(element);
        String partitionName =
                element.getPartitionName() == null ? "_default" : element.getPartitionName();
        this.milvusDataCache.computeIfAbsent(partitionName, k -> new ArrayList<>());
        milvusDataCache.get(partitionName).add(data);
        writeCache.incrementAndGet();
    }

    @Override
    public boolean needFlush() {
        return this.writeCache.get() >= this.batchSize;
    }

    @Override
    public void flush() throws Exception {
        log.info("Starting to put {} records to Milvus.", this.writeCache.get());
        // Flush the batch writer
        // Get the number of records completed
        if (this.milvusDataCache.isEmpty()) {
            return;
        }
        writeData2Collection();
        log.info(
                "Successfully put {} records to Milvus. Total records written: {}",
                this.writeCache.get(),
                this.writeCount.get());
        this.milvusDataCache = new HashMap<>();
        this.writeCache.set(0L);
    }

    @Override
    public void close() throws Exception {
        String collectionName = catalogTable.getTablePath().getTableName();
        // set rate limit
        Map<String, String> properties = new HashMap<>();
        properties.put("collection.insertRate.max.mb", "-1");
        properties.put("collection.upsertRate.max.mb", "-1");
        AlterCollectionReq alterCollectionReq =
                AlterCollectionReq.builder()
                        .collectionName(collectionName)
                        .properties(properties)
                        .build();
        milvusClient.alterCollection(alterCollectionReq);
        this.milvusClient.close(10);
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
            Gson gson = new Gson();
            Object object = MilvusConvertUtils.convertBySeaTunnelType(fieldType, value);
            data.add(fieldName, gson.toJsonTree(object));
        }
        return data;
    }

    private void writeData2Collection() throws Exception {
        try {
            for (String partitionName : milvusDataCache.keySet()) {
                // default to use upsertReq, but upsert only works when autoID is disabled
                List<JsonObject> data = milvusDataCache.get(partitionName);
                if (Objects.equals(partitionName, "_default") || hasPartitionKey) {
                    partitionName = null;
                }
                if (enableUpsert && !autoId) {
                    upsertWrite(partitionName, data);
                } else {
                    insertWrite(partitionName, data);
                }
            }
        } catch (Exception e) {
            log.error("write data to Milvus failed", e);
            log.error("error data: " + milvusDataCache);
            throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_DATA_FAIL);
        }
        writeCount.addAndGet(this.writeCache.get());
    }

    private void upsertWrite(String partitionName, List<JsonObject> data)
            throws InterruptedException {
        UpsertReq upsertReq =
                UpsertReq.builder().collectionName(this.collectionName).data(data).build();
        if (StringUtils.isNotEmpty(partitionName)) {
            upsertReq.setPartitionName(partitionName);
        }
        try {
            milvusClient.upsert(upsertReq);
        } catch (Exception e) {
            if (e.getMessage().contains("rate limit exceeded")
                    || e.getMessage().contains("received message larger than max")) {
                if (data.size() > 10) {
                    log.warn("upsert data failed, retry in smaller chunks: {} ", data.size() / 2);
                    this.batchSize = this.batchSize / 2;
                    log.info("sleep 1 minute to avoid rate limit");
                    // sleep 1 minute to avoid rate limit
                    Thread.sleep(60000);
                    log.info("sleep 1 minute success");
                    // Split the data and retry in smaller chunks
                    List<JsonObject> firstHalf = data.subList(0, data.size() / 2);
                    List<JsonObject> secondHalf = data.subList(data.size() / 2, data.size());
                    upsertWrite(partitionName, firstHalf);
                    upsertWrite(partitionName, secondHalf);
                } else {
                    // If the data size is 10, throw the exception to avoid infinite recursion
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.WRITE_DATA_FAIL, e.getMessage(), e);
                }
            }
        }
        log.info("upsert data success");
    }

    private void insertWrite(String partitionName, List<JsonObject> data) {
        InsertReq insertReq =
                InsertReq.builder().collectionName(this.collectionName).data(data).build();
        if (StringUtils.isNotEmpty(partitionName)) {
            insertReq.setPartitionName(partitionName);
        }
        try {
            milvusClient.insert(insertReq);
        } catch (Exception e) {
            if (e.getMessage().contains("rate limit exceeded")
                    || e.getMessage().contains("received message larger than max")) {
                if (data.size() > 10) {
                    log.warn("insert data failed, retry in smaller chunks: {} ", data.size() / 2);
                    // Split the data and retry in smaller chunks
                    List<JsonObject> firstHalf = data.subList(0, data.size() / 2);
                    List<JsonObject> secondHalf = data.subList(data.size() / 2, data.size());
                    this.batchSize = this.batchSize / 2;
                    insertWrite(partitionName, firstHalf);
                    insertWrite(partitionName, secondHalf);
                } else {
                    // If the data size is 1, throw the exception to avoid infinite recursion
                    throw e;
                }
            }
        }
    }
}
