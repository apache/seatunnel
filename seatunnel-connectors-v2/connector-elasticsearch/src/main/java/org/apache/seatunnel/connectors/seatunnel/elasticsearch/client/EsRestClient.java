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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class EsRestClient {

    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;

    private final RestClient restClient;

    private final ObjectMapper mapper = new ObjectMapper();

    private EsRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public static EsRestClient createInstance(Config pluginConfig) {
        List<String> hosts = pluginConfig.getStringList(EsClusterConnectionConfig.HOSTS.key());
        String username = null;
        String password = null;
        if (pluginConfig.hasPath(EsClusterConnectionConfig.USERNAME.key())) {
            username = pluginConfig.getString(EsClusterConnectionConfig.USERNAME.key());
            if (pluginConfig.hasPath(EsClusterConnectionConfig.PASSWORD.key())) {
                password = pluginConfig.getString(EsClusterConnectionConfig.PASSWORD.key());
            }
        }
        return createInstance(hosts, username, password);
    }

    public static EsRestClient createInstance(List<String> hosts, String username, String password) {
        RestClientBuilder restClientBuilder = getRestClientBuilder(hosts, username, password);
        return new EsRestClient(restClientBuilder.build());
    }

    private static RestClientBuilder getRestClientBuilder(List<String> hosts, String username, String password) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            String[] hostInfo = hosts.get(i).replace("http://", "").split(":");
            httpHosts[i] = new HttpHost(hostInfo[0], Integer.parseInt(hostInfo[1]));
        }

        RestClientBuilder builder = RestClient.builder(httpHosts)
            .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
                .setSocketTimeout(SOCKET_TIMEOUT));

        if (StringUtils.isNotEmpty(username)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return builder;
    }

    public BulkResponse bulk(String requestBody) {
        Request request = new Request("POST", "_bulk");
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR, "bulk es Response is null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                ObjectMapper objectMapper = new ObjectMapper();
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = objectMapper.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR, String.format("bulk es response status code=%d,request boy=%s", response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR, String.format("bulk es error,request boy=%s", requestBody), e);
        }
    }

    /**
     * @return version.number, example:2.0.0
     */
    public String getClusterVersion() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(result);
            JsonNode versionNode = jsonNode.get("version");
            return versionNode.get("number").asText();
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_ES_VERSION_FAILED, "fail to get elasticsearch version.", e);
        }
    }

    public void close() {
        try {
            restClient.close();
        } catch (IOException e) {
            log.warn("close elasticsearch connection error", e);
        }
    }

    /**
     * first time to request search documents by scroll
     * call /${index}/_search?scroll=${scroll}
     *
     * @param index      index name
     * @param source     select fields
     * @param scrollTime such as:1m
     * @param scrollSize fetch documents count in one request
     */
    public ScrollResult searchByScroll(String index, List<String> source, String scrollTime, int scrollSize) {
        Map<String, Object> param = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<String, String>());
        param.put("query", query);
        param.put("_source", source);
        param.put("sort", new String[]{"_doc"});
        param.put("size", scrollSize);
        String endpoint = index + "/_search?scroll=" + scrollTime;
        ScrollResult scrollResult = getDocsFromScrollRequest(endpoint, JsonUtils.toJsonString(param));
        return scrollResult;
    }

    /**
     * scroll to get result
     * call _search/scroll
     *
     * @param scrollId   the scroll id of the last request
     * @param scrollTime such as:1m
     */
    public ScrollResult searchWithScrollId(String scrollId, String scrollTime) {
        Map<String, String> param = new HashMap<>();
        param.put("scroll_id", scrollId);
        param.put("scroll", scrollTime);
        ScrollResult scrollResult = getDocsFromScrollRequest("_search/scroll", JsonUtils.toJsonString(param));
        return scrollResult;
    }

    private ScrollResult getDocsFromScrollRequest(String endpoint, String requestBody) {
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR, "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);

                JsonNode shards = responseJson.get("_shards");
                int totalShards = shards.get("total").intValue();
                int successful = shards.get("successful").intValue();
                Asserts.check(totalShards == successful, String.format("POST %s,total shards(%d)!= successful shards(%d)", endpoint, totalShards, successful));

                ScrollResult scrollResult = getDocsFromScrollResponse(responseJson);
                return scrollResult;
            } else {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                    String.format("POST %s response status code=%d,request boy=%s", endpoint, response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                String.format("POST %s error,request boy=%s", endpoint, requestBody), e);

        }
    }

    private ScrollResult getDocsFromScrollResponse(ObjectNode responseJson) {
        ScrollResult scrollResult = new ScrollResult();
        String scrollId = responseJson.get("_scroll_id").asText();
        scrollResult.setScrollId(scrollId);

        JsonNode hitsNode = responseJson.get("hits").get("hits");
        List<Map<String, Object>> docs = new ArrayList<>(hitsNode.size());
        scrollResult.setDocs(docs);

        Iterator<JsonNode> iter = hitsNode.iterator();
        while (iter.hasNext()) {
            Map<String, Object> doc = new HashMap<>();
            JsonNode hitNode = iter.next();
            doc.put("_index", hitNode.get("_index").textValue());
            doc.put("_id", hitNode.get("_id").textValue());
            JsonNode source = hitNode.get("_source");
            for (Iterator<Map.Entry<String, JsonNode>> iterator = source.fields(); iterator.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                String fieldName = entry.getKey();
                if (entry.getValue() instanceof TextNode) {
                    doc.put(fieldName, entry.getValue().textValue());
                } else {
                    doc.put(fieldName, entry.getValue());
                }
            }
            docs.add(doc);
        }
        return scrollResult;
    }

    public List<IndexDocsCount> getIndexDocsCount(String index) {
        String endpoint = String.format("_cat/indices/%s?h=index,docsCount&format=json", index);
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                    "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                List<IndexDocsCount> indexDocsCounts = JsonUtils.toList(entity, IndexDocsCount.class);
                return indexDocsCounts;
            } else {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                    String.format("GET %s response status code=%d", endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
    }

    /**
     * get es field name and type mapping realtion
     *
     * @param index index name
     * @return {key-> field name,value->es type}
     */
    public Map<String, String> getFieldTypeMapping(String index, List<String> source) {
        String endpoint = String.format("%s/_mappings", index);
        Request request = new Request("GET", endpoint);
        Map<String, String> mapping = new HashMap<>();
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                    "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                    String.format("GET %s response status code=%d", endpoint, response.getStatusLine().getStatusCode()));
            }
            String entity = EntityUtils.toString(response.getEntity());
            log.info(String.format("GET %s respnse=%s", endpoint, entity));
            ObjectNode responseJson = JsonUtils.parseObject(entity);
            for (Iterator<JsonNode> it = responseJson.elements(); it.hasNext(); ) {
                JsonNode indexProperty = it.next();
                JsonNode mappingsProperty = indexProperty.get("mappings");
                if (mappingsProperty.has("mappingsProperty")) {
                    JsonNode properties = mappingsProperty.get("properties");
                    mapping = getFieldTypeMappingFromProperties(properties, source);
                } else {
                    for (Iterator<JsonNode> iter = mappingsProperty.iterator(); iter.hasNext(); ) {
                        JsonNode typeNode = iter.next();
                        JsonNode properties = typeNode.get("properties");
                        mapping.putAll(getFieldTypeMappingFromProperties(properties, source));
                    }
                }

            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
        return mapping;
    }

    private static Map<String, String> getFieldTypeMappingFromProperties(JsonNode properties, List<String> source) {
        Map<String, String> mapping = new HashMap<>();
        for (String field : source) {
            JsonNode fieldProperty = properties.get(field);
            if (fieldProperty == null) {
                mapping.put(field, "text");
            } else {
                if (fieldProperty.has("type")) {
                    String type = fieldProperty.get("type").asText();
                    mapping.put(field, type);
                } else {
                    log.warn(String.format("fail to get elasticsearch field %s mapping type,so give a default type text", field));
                    mapping.put(field, "text");
                }
            }
        }
        return mapping;
    }

}
