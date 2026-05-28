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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.DynamicMetadataDataSource;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.engine.server.rest.RestConstant.MESSAGE;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STATUS;

@Slf4j
public class DynamicMetadataDataSourceService extends BaseService {

    /** Field names for JSON serialization, aligned with MetadataDataSource fields */
    public static final String METADATA_DATASOURCE_ID = "metadataDatasourceId";

    public static final String CONNECTOR_TYPE = "connectorType";
    public static final String PROPERTIES = "properties";
    public static final String CREATE_TIME = "createTime";
    public static final String UPDATE_TIME = "updateTime";
    private static final String MASKED_VALUE = "******";
    private static final Set<String> SENSITIVE_KEYS =
            new HashSet<>(Arrays.asList("password", "secret_key"));

    public DynamicMetadataDataSourceService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /**
     * Create a new metadata datasource.
     *
     * @param requestBody the request body containing datasource information
     * @return JsonObject with creation result
     */
    public JsonObject createDatasource(byte[] requestBody) {
        Map<String, Object> params = JsonUtils.toMap(requestHandle(requestBody));

        String datasourceId = (String) params.get(METADATA_DATASOURCE_ID);
        String type = (String) params.get(CONNECTOR_TYPE);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) params.get(PROPERTIES);

        // Validate required fields
        if (datasourceId == null || datasourceId.trim().isEmpty()) {
            return new JsonObject()
                    .add(STATUS, "error")
                    .add(MESSAGE, "metadataDatasourceId is required");
        }
        if (type == null || type.trim().isEmpty()) {
            return new JsonObject().add(STATUS, "error").add(MESSAGE, "connectorType is required");
        }

        IMap<String, DynamicMetadataDataSource> datasourceIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_METADATA_DATASOURCE);

        DynamicMetadataDataSource metadataDataSource =
                new DynamicMetadataDataSource(datasourceId, type, properties);
        DynamicMetadataDataSource existing =
                datasourceIMap.putIfAbsent(datasourceId, metadataDataSource);
        if (existing != null) {
            return new JsonObject()
                    .add(STATUS, "error")
                    .add(MESSAGE, "Datasource with id '" + datasourceId + "' already exists");
        }

        log.info("Created metadata datasource: id={}, type={}", datasourceId, type);

        return new JsonObject()
                .add(STATUS, "success")
                .add(METADATA_DATASOURCE_ID, datasourceId)
                .add(MESSAGE, "Datasource created successfully");
    }

    /**
     * Get a metadata datasource by id.
     *
     * @param datasourceId the datasource id
     * @return JsonObject with datasource information
     */
    public JsonObject getDatasource(String datasourceId) {
        if (datasourceId == null || datasourceId.trim().isEmpty()) {
            return new JsonObject().add(STATUS, "error").add(MESSAGE, "datasourceId is required");
        }

        IMap<String, DynamicMetadataDataSource> datasourceIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_METADATA_DATASOURCE);

        DynamicMetadataDataSource metadataDataSource = datasourceIMap.get(datasourceId);
        if (metadataDataSource == null) {
            return new JsonObject()
                    .add(STATUS, "error")
                    .add(MESSAGE, "Datasource with id '" + datasourceId + "' not found");
        }

        return convertToJson(metadataDataSource);
    }

    /**
     * List all metadata datasources.
     *
     * @return JsonArray with all datasources
     */
    public JsonArray listDatasources() {
        IMap<String, DynamicMetadataDataSource> datasourceIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_METADATA_DATASOURCE);

        JsonArray result = new JsonArray();
        datasourceIMap
                .values()
                .forEach(
                        datasource -> {
                            result.add(convertToJson(datasource));
                        });

        return result;
    }

    /**
     * Update a metadata datasource.
     *
     * @param datasourceId the datasource id
     * @param requestBody the request body containing updated information
     * @return JsonObject with update result
     */
    public JsonObject updateDatasource(String datasourceId, byte[] requestBody) {
        if (datasourceId == null || datasourceId.trim().isEmpty()) {
            return new JsonObject()
                    .add(STATUS, "error")
                    .add(MESSAGE, "metadataDatasourceId is required");
        }

        Map<String, Object> params = JsonUtils.toMap(requestHandle(requestBody));
        String type = (String) params.get(CONNECTOR_TYPE);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) params.get(PROPERTIES);

        IMap<String, DynamicMetadataDataSource> datasourceIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_METADATA_DATASOURCE);

        datasourceIMap.lock(datasourceId);
        try {
            DynamicMetadataDataSource existingDatasource = datasourceIMap.get(datasourceId);
            if (existingDatasource == null) {
                return new JsonObject()
                        .add(STATUS, "error")
                        .add(MESSAGE, "Datasource with id '" + datasourceId + "' not found");
            }

            if (type != null && !type.trim().isEmpty()) {
                existingDatasource.setConnectorType(type);
            }
            if (properties != null && !properties.isEmpty()) {
                existingDatasource.updateProperties(properties);
            }

            datasourceIMap.put(datasourceId, existingDatasource);
        } finally {
            datasourceIMap.unlock(datasourceId);
        }

        log.info("Updated metadata datasource: id={}", datasourceId);

        return new JsonObject()
                .add(STATUS, "success")
                .add(METADATA_DATASOURCE_ID, datasourceId)
                .add(MESSAGE, "Datasource updated successfully");
    }

    /**
     * Delete a metadata datasource.
     *
     * @param datasourceId the datasource id
     * @return JsonObject with deletion result
     */
    public JsonObject deleteDatasource(String datasourceId) {
        if (datasourceId == null || datasourceId.trim().isEmpty()) {
            return new JsonObject()
                    .add(STATUS, "error")
                    .add(MESSAGE, "metadataDatasourceId is required");
        }

        IMap<String, DynamicMetadataDataSource> datasourceIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_METADATA_DATASOURCE);

        DynamicMetadataDataSource deleted = datasourceIMap.remove(datasourceId);
        if (deleted == null) {
            return new JsonObject()
                    .add(STATUS, "error")
                    .add(MESSAGE, "Datasource with id '" + datasourceId + "' not found");
        }

        log.info("Deleted metadata datasource: id={}", datasourceId);

        return new JsonObject()
                .add(STATUS, "success")
                .add(METADATA_DATASOURCE_ID, datasourceId)
                .add(MESSAGE, "Datasource deleted successfully");
    }

    /**
     * Convert MetadataDataSource to JsonObject.
     *
     * @param datasource the metadata datasource
     * @return JsonObject representation
     */
    private JsonObject convertToJson(DynamicMetadataDataSource datasource) {
        JsonObject result = new JsonObject();
        result.add(METADATA_DATASOURCE_ID, datasource.getMetadataDatasourceId());
        result.add(CONNECTOR_TYPE, datasource.getConnectorType());

        // Add properties
        JsonObject propertiesJson = new JsonObject();
        if (datasource.getProperties() != null) {
            datasource
                    .getProperties()
                    .forEach(
                            (key, value) -> {
                                propertiesJson.add(
                                        key,
                                        isSensitiveKey(key)
                                                ? MASKED_VALUE
                                                : value != null ? value.toString() : "");
                            });
        }
        result.add(PROPERTIES, propertiesJson);

        result.add(CREATE_TIME, datasource.getCreateTime());
        result.add(UPDATE_TIME, datasource.getUpdateTime());

        return result;
    }

    private boolean isSensitiveKey(String key) {
        String lowerKey = key.toLowerCase();
        return SENSITIVE_KEYS.stream().anyMatch(lowerKey::contains);
    }
}
