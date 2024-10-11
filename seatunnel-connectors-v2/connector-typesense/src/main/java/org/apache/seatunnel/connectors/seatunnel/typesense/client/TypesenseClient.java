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

package org.apache.seatunnel.connectors.seatunnel.typesense.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.typesense.util.URLParamsConverter;

import org.apache.commons.lang3.StringUtils;

import org.typesense.api.Client;
import org.typesense.api.Collections;
import org.typesense.api.Configuration;
import org.typesense.api.FieldTypes;
import org.typesense.model.CollectionResponse;
import org.typesense.model.CollectionSchema;
import org.typesense.model.DeleteDocumentsParameters;
import org.typesense.model.Field;
import org.typesense.model.ImportDocumentsParameters;
import org.typesense.model.SearchParameters;
import org.typesense.model.SearchResult;
import org.typesense.resources.Node;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SourceConfig.QUERY_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.CREATE_COLLECTION_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.DELETE_COLLECTION_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.DROP_COLLECTION_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.FIELD_TYPE_MAPPING_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.INSERT_DOC_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.QUERY_COLLECTION_EXISTS_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.QUERY_COLLECTION_LIST_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.QUERY_COLLECTION_NUM_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.TRUNCATE_COLLECTION_ERROR;

@Slf4j
public class TypesenseClient {
    private final Client tsClient;

    TypesenseClient(Client tsClient) {
        this.tsClient = tsClient;
    }

    public static TypesenseClient createInstance(ReadonlyConfig config) {
        List<String> hosts = config.get(TypesenseConnectionConfig.HOSTS);
        String protocol = config.get(TypesenseConnectionConfig.PROTOCOL);
        String apiKey = config.get(TypesenseConnectionConfig.APIKEY);
        return createInstance(hosts, apiKey, protocol);
    }

    public static TypesenseClient createInstance(
            List<String> hosts, String apiKey, String protocol) {
        List<Node> nodes = new ArrayList<>();

        hosts.stream()
                .map(host -> host.split(":"))
                .forEach(
                        split ->
                                nodes.add(
                                        new Node(
                                                protocol,
                                                split[0],
                                                StringUtils.isBlank(split[1])
                                                        ? "8018"
                                                        : split[1])));

        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(5), apiKey);
        Client client = new Client(configuration);
        return new TypesenseClient(client);
    }

    public void insert(String collection, List<String> documentList) {

        ImportDocumentsParameters queryParameters = new ImportDocumentsParameters();
        queryParameters.action("upsert");
        String text = "";
        for (String s : documentList) {
            text = text + s + "\n";
        }
        try {
            tsClient.collections(collection).documents().import_(text, queryParameters);
        } catch (Exception e) {
            log.error(INSERT_DOC_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    INSERT_DOC_ERROR, INSERT_DOC_ERROR.getDescription());
        }
    }

    public SearchResult search(String collection, String query, int offset) throws Exception {
        return search(collection, query, offset, QUERY_BATCH_SIZE.defaultValue());
    }

    public SearchResult search(String collection, String query, int offset, int pageSize)
            throws Exception {
        SearchParameters searchParameters;
        if (StringUtils.isNotBlank(query)) {
            String jsonQuery = URLParamsConverter.convertParamsToJson(query);
            ObjectMapper objectMapper = new ObjectMapper();
            searchParameters = objectMapper.readValue(jsonQuery, SearchParameters.class);
        } else {
            searchParameters = new SearchParameters().q("*");
        }
        log.debug("Typesense query param:{}", searchParameters);
        searchParameters.offset(offset);
        searchParameters.perPage(pageSize);
        SearchResult searchResult =
                tsClient.collections(collection).documents().search(searchParameters);
        return searchResult;
    }

    public boolean collectionExists(String collection) {
        try {
            Collections collections = tsClient.collections();
            CollectionResponse[] collectionResponses = collections.retrieve();
            for (CollectionResponse collectionRespons : collectionResponses) {
                String collectionName = collectionRespons.getName();
                if (collection.equals(collectionName)) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.error(QUERY_COLLECTION_EXISTS_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    QUERY_COLLECTION_EXISTS_ERROR, QUERY_COLLECTION_EXISTS_ERROR.getDescription());
        }
        return false;
    }

    public List<String> collectionList() {
        try {
            Collections collections = tsClient.collections();
            CollectionResponse[] collectionResponses = collections.retrieve();
            List<String> list = new ArrayList<>();
            for (CollectionResponse collectionRespons : collectionResponses) {
                String collectionName = collectionRespons.getName();
                list.add(collectionName);
            }
            return list;
        } catch (Exception e) {
            log.error(QUERY_COLLECTION_LIST_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    QUERY_COLLECTION_LIST_ERROR, QUERY_COLLECTION_LIST_ERROR.getDescription());
        }
    }

    public Map<String, String> getField(String collection) {
        if (collectionExists(collection)) {
            Map<String, String> fieldMap = new HashMap<>();
            try {
                CollectionResponse collectionResponse = tsClient.collections(collection).retrieve();
                List<Field> fields = collectionResponse.getFields();
                for (Field field : fields) {
                    String fieldName = field.getName();
                    String type = field.getType();
                    fieldMap.put(fieldName, type);
                }
            } catch (Exception e) {
                log.error(FIELD_TYPE_MAPPING_ERROR.getDescription());
                throw new TypesenseConnectorException(
                        FIELD_TYPE_MAPPING_ERROR, FIELD_TYPE_MAPPING_ERROR.getDescription());
            }
            return fieldMap;
        } else {
            return null;
        }
    }

    public Map<String, BasicTypeDefine<TypesenseType>> getFieldTypeMapping(String collection) {
        Map<String, BasicTypeDefine<TypesenseType>> allTypesenseSearchFieldTypeInfoMap =
                new HashMap<>();
        try {
            CollectionResponse collectionResponse = tsClient.collections(collection).retrieve();
            List<Field> fields = collectionResponse.getFields();
            for (Field field : fields) {
                String fieldName = field.getName();
                String type = field.getType();
                BasicTypeDefine.BasicTypeDefineBuilder<TypesenseType> typeDefine =
                        BasicTypeDefine.<TypesenseType>builder()
                                .name(fieldName)
                                .columnType(type)
                                .dataType(type)
                                .nativeType(new TypesenseType(type, new HashMap<>()));
                allTypesenseSearchFieldTypeInfoMap.put(fieldName, typeDefine.build());
            }
        } catch (Exception e) {
            log.error(FIELD_TYPE_MAPPING_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    FIELD_TYPE_MAPPING_ERROR, FIELD_TYPE_MAPPING_ERROR.getDescription());
        }
        return allTypesenseSearchFieldTypeInfoMap;
    }

    public boolean createCollection(String collection) {
        if (collectionExists(collection)) {
            return true;
        }
        List<Field> fields = new ArrayList<>();
        fields.add(new Field().name(".*").type(FieldTypes.AUTO));
        return createCollection(collection, fields);
    }

    public boolean createCollection(String collection, List<Field> fields) {
        CollectionSchema collectionSchema = new CollectionSchema();
        collectionSchema.name(collection).fields(fields).enableNestedFields(true);
        try {
            tsClient.collections().create(collectionSchema);
            return true;
        } catch (Exception e) {
            log.error(CREATE_COLLECTION_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    CREATE_COLLECTION_ERROR, CREATE_COLLECTION_ERROR.getDescription());
        }
    }

    public boolean dropCollection(String collection) {
        try {
            tsClient.collections(collection).delete();
            return true;
        } catch (Exception e) {
            log.error(DROP_COLLECTION_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    DROP_COLLECTION_ERROR, DROP_COLLECTION_ERROR.getDescription());
        }
    }

    public boolean truncateCollectionData(String collection) {
        DeleteDocumentsParameters deleteDocumentsParameters = new DeleteDocumentsParameters();
        deleteDocumentsParameters.filterBy("id:!=1||id:=1");
        try {
            tsClient.collections(collection).documents().delete(deleteDocumentsParameters);
        } catch (Exception e) {
            log.error(TRUNCATE_COLLECTION_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    TRUNCATE_COLLECTION_ERROR, TRUNCATE_COLLECTION_ERROR.getDescription());
        }
        return true;
    }

    public boolean deleteCollectionData(String collection, String id) {
        try {
            tsClient.collections(collection).documents(id).delete();
        } catch (Exception e) {
            log.error(DELETE_COLLECTION_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    DELETE_COLLECTION_ERROR, DELETE_COLLECTION_ERROR.getDescription());
        }
        return true;
    }

    public long collectionDocNum(String collection) {
        SearchParameters q = new SearchParameters().q("*");
        try {
            SearchResult searchResult = tsClient.collections(collection).documents().search(q);
            return searchResult.getFound();
        } catch (Exception e) {
            log.error(QUERY_COLLECTION_NUM_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    QUERY_COLLECTION_NUM_ERROR, QUERY_COLLECTION_NUM_ERROR.getDescription());
        }
    }
}
