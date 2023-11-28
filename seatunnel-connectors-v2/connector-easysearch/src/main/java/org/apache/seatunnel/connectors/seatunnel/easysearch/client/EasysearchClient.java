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

package org.apache.seatunnel.connectors.seatunnel.easysearch.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.EasysearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.easysearch.util.SSLUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;

import org.easysearch.client.Request;
import org.easysearch.client.Response;
import org.easysearch.client.RestClient;
import org.easysearch.client.RestClientBuilder;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class EasysearchClient {

    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;

    private final RestClient restClient;

    private EasysearchClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public static EasysearchClient createInstance(Config pluginConfig) {
        List<String> hosts = pluginConfig.getStringList(EzsClusterConnectionConfig.HOSTS.key());
        Optional<String> username = Optional.empty();
        Optional<String> password = Optional.empty();
        if (pluginConfig.hasPath(EzsClusterConnectionConfig.USERNAME.key())) {
            username =
                    Optional.of(pluginConfig.getString(EzsClusterConnectionConfig.USERNAME.key()));
            if (pluginConfig.hasPath(EzsClusterConnectionConfig.PASSWORD.key())) {
                password =
                        Optional.of(
                                pluginConfig.getString(EzsClusterConnectionConfig.PASSWORD.key()));
            }
        }
        Optional<String> keystorePath = Optional.empty();
        Optional<String> keystorePassword = Optional.empty();
        Optional<String> truststorePath = Optional.empty();
        Optional<String> truststorePassword = Optional.empty();
        boolean tlsVerifyCertificate =
                EzsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE.defaultValue();
        if (pluginConfig.hasPath(EzsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE.key())) {
            tlsVerifyCertificate =
                    pluginConfig.getBoolean(
                            EzsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE.key());
        }
        if (tlsVerifyCertificate) {
            if (pluginConfig.hasPath(EzsClusterConnectionConfig.TLS_KEY_STORE_PATH.key())) {
                keystorePath =
                        Optional.of(
                                pluginConfig.getString(
                                        EzsClusterConnectionConfig.TLS_KEY_STORE_PATH.key()));
            }
            if (pluginConfig.hasPath(EzsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD.key())) {
                keystorePassword =
                        Optional.of(
                                pluginConfig.getString(
                                        EzsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD.key()));
            }
            if (pluginConfig.hasPath(EzsClusterConnectionConfig.TLS_TRUST_STORE_PATH.key())) {
                truststorePath =
                        Optional.of(
                                pluginConfig.getString(
                                        EzsClusterConnectionConfig.TLS_TRUST_STORE_PATH.key()));
            }
            if (pluginConfig.hasPath(EzsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD.key())) {
                truststorePassword =
                        Optional.of(
                                pluginConfig.getString(
                                        EzsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD.key()));
            }
        }
        boolean tlsVerifyHostnames = EzsClusterConnectionConfig.TLS_VERIFY_HOSTNAME.defaultValue();
        if (pluginConfig.hasPath(EzsClusterConnectionConfig.TLS_VERIFY_HOSTNAME.key())) {
            tlsVerifyHostnames =
                    pluginConfig.getBoolean(EzsClusterConnectionConfig.TLS_VERIFY_HOSTNAME.key());
        }
        return createInstance(
                hosts,
                username,
                password,
                tlsVerifyCertificate,
                tlsVerifyHostnames,
                keystorePath,
                keystorePassword,
                truststorePath,
                truststorePassword);
    }

    public static EasysearchClient createInstance(
            List<String> hosts,
            Optional<String> username,
            Optional<String> password,
            boolean tlsVerifyCertificate,
            boolean tlsVerifyHostnames,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword) {
        RestClientBuilder restClientBuilder =
                getRestClientBuilder(
                        hosts,
                        username,
                        password,
                        tlsVerifyCertificate,
                        tlsVerifyHostnames,
                        keystorePath,
                        keystorePassword,
                        truststorePath,
                        truststorePassword);
        return new EasysearchClient(restClientBuilder.build());
    }

    private static RestClientBuilder getRestClientBuilder(
            List<String> hosts,
            Optional<String> username,
            Optional<String> password,
            boolean tlsVerifyCertificate,
            boolean tlsVerifyHostnames,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            httpHosts[i] = HttpHost.create(hosts.get(i));
        }

        RestClientBuilder restClientBuilder =
                RestClient.builder(httpHosts)
                        .setRequestConfigCallback(
                                requestConfigBuilder ->
                                        requestConfigBuilder
                                                .setConnectionRequestTimeout(
                                                        CONNECTION_REQUEST_TIMEOUT)
                                                .setSocketTimeout(SOCKET_TIMEOUT));

        restClientBuilder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    if (username.isPresent()) {
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials(username.get(), password.get()));
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    try {
                        if (tlsVerifyCertificate) {
                            Optional<SSLContext> sslContext =
                                    SSLUtils.buildSSLContext(
                                            keystorePath,
                                            keystorePassword,
                                            truststorePath,
                                            truststorePassword);
                            sslContext.ifPresent(e -> httpClientBuilder.setSSLContext(e));
                        } else {
                            SSLContext sslContext =
                                    SSLContexts.custom()
                                            .loadTrustMaterial(new TrustAllStrategy())
                                            .build();
                            httpClientBuilder.setSSLContext(sslContext);
                        }
                        if (!tlsVerifyHostnames) {
                            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return httpClientBuilder;
                });
        return restClientBuilder;
    }

    private static Map<String, String> getFieldTypeMappingFromProperties(
            JsonNode properties, List<String> source) {
        Map<String, String> allEasysearchFieldTypeInfoMap = new HashMap<>();
        properties
                .fields()
                .forEachRemaining(
                        entry -> {
                            String fieldName = entry.getKey();
                            JsonNode fieldProperty = entry.getValue();
                            if (fieldProperty.has("type")) {
                                allEasysearchFieldTypeInfoMap.put(
                                        fieldName, fieldProperty.get("type").asText());
                            }
                        });
        if (CollectionUtils.isEmpty(source)) {
            return allEasysearchFieldTypeInfoMap;
        }

        return source.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                fieldName -> {
                                    String fieldType = allEasysearchFieldTypeInfoMap.get(fieldName);
                                    if (fieldType == null) {
                                        log.warn(
                                                "fail to get easysearch field {} mapping type,so give a default type text",
                                                fieldName);
                                        return "text";
                                    }
                                    return fieldType;
                                }));
    }

    public BulkResponse bulk(String requestBody) {
        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        "bulk ezs Response is null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                ObjectMapper objectMapper = new ObjectMapper();
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = objectMapper.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        String.format(
                                "bulk ezs response status code=%d,request boy=%s",
                                response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                    String.format("bulk ezs error,request boy=%s", requestBody),
                    e);
        }
    }

    public EasysearchClusterInfo getClusterInfo() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(result);
            JsonNode versionNode = jsonNode.get("version");
            return EasysearchClusterInfo.builder()
                    .clusterVersion(versionNode.get("number").asText())
                    .distribution(
                            Optional.ofNullable(versionNode.get("distribution"))
                                    .map(e -> e.asText())
                                    .orElse(null))
                    .build();
        } catch (IOException e) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.GET_EZS_VERSION_FAILED,
                    "fail to get easysearch version.",
                    e);
        }
    }

    public void close() {
        try {
            restClient.close();
        } catch (IOException e) {
            log.warn("close easysearch connection error", e);
        }
    }

    /**
     * first time to request search documents by scroll call /${index}/_search?scroll=${scroll}
     *
     * @param index index name
     * @param source select fields
     * @param scrollTime such as:1m
     * @param scrollSize fetch documents count in one request
     */
    public ScrollResult searchByScroll(
            String index,
            List<String> source,
            Map<String, Object> query,
            String scrollTime,
            int scrollSize) {
        Map<String, Object> param = new HashMap<>();
        param.put("query", query);
        param.put("_source", source);
        param.put("sort", new String[] {"_doc"});
        param.put("size", scrollSize);
        String endpoint = "/" + index + "/_search?scroll=" + scrollTime;
        ScrollResult scrollResult =
                getDocsFromScrollRequest(endpoint, JsonUtils.toJsonString(param));
        return scrollResult;
    }

    /**
     * scroll to get result call _search/scroll
     *
     * @param scrollId the scroll id of the last request
     * @param scrollTime such as:1m
     */
    public ScrollResult searchWithScrollId(String scrollId, String scrollTime) {
        Map<String, String> param = new HashMap<>();
        param.put("scroll_id", scrollId);
        param.put("scroll", scrollTime);
        ScrollResult scrollResult =
                getDocsFromScrollRequest("/_search/scroll", JsonUtils.toJsonString(param));
        return scrollResult;
    }

    private ScrollResult getDocsFromScrollRequest(String endpoint, String requestBody) {
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);

                JsonNode shards = responseJson.get("_shards");
                int totalShards = shards.get("total").intValue();
                int successful = shards.get("successful").intValue();
                Asserts.check(
                        totalShards == successful,
                        String.format(
                                "POST %s,total shards(%d)!= successful shards(%d)",
                                endpoint, totalShards, successful));

                ScrollResult scrollResult = getDocsFromScrollResponse(responseJson);
                return scrollResult;
            } else {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "POST %s response status code=%d,request boy=%s",
                                endpoint, response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                    String.format("POST %s error,request boy=%s", endpoint, requestBody),
                    e);
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
            for (Iterator<Map.Entry<String, JsonNode>> iterator = source.fields();
                    iterator.hasNext(); ) {
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
        String endpoint = String.format("/_cat/indices/%s?h=index,docsCount&format=json", index);
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                List<IndexDocsCount> indexDocsCounts =
                        JsonUtils.toList(entity, IndexDocsCount.class);
                return indexDocsCounts;
            } else {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
    }

    public List<String> listIndex() {
        String endpoint = "/_cat/indices?format=json";
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.LIST_INDEX_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                return JsonUtils.toList(entity, Map.class).stream()
                        .map(map -> map.get("index").toString())
                        .collect(Collectors.toList());
            } else {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.LIST_INDEX_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.LIST_INDEX_FAILED, ex);
        }
    }

    // todo: We don't support set the index mapping now.
    public void createIndex(String indexName) {
        String endpoint = String.format("/%s", indexName);
        Request request = new Request("PUT", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        "PUT " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        String.format(
                                "PUT %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.CREATE_INDEX_FAILED, ex);
        }
    }

    public void dropIndex(String tableName) {
        String endpoint = String.format("/%s", tableName);
        Request request = new Request("DELETE", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.DROP_INDEX_FAILED,
                        "DELETE " + endpoint + " response null");
            }
            // todo: if the index doesn't exist, the response status code is 200?
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return;
            } else {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.DROP_INDEX_FAILED,
                        String.format(
                                "DELETE %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.DROP_INDEX_FAILED, ex);
        }
    }

    /**
     * get ezs field name and type mapping realtion
     *
     * @param index index name
     * @return {key-> field name,value->ezs type}
     */
    public Map<String, String> getFieldTypeMapping(String index, List<String> source) {
        String endpoint = String.format("/%s/_mappings", index);
        Request request = new Request("GET", endpoint);
        Map<String, String> mapping = new HashMap<>();
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new EasysearchConnectorException(
                        EasysearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
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
                    for (JsonNode typeNode : mappingsProperty) {
                        JsonNode properties;
                        if (typeNode.has("properties")) {
                            properties = typeNode.get("properties");
                        } else {
                            properties = typeNode;
                        }
                        mapping.putAll(getFieldTypeMappingFromProperties(properties, source));
                    }
                }
            }
        } catch (IOException ex) {
            throw new EasysearchConnectorException(
                    EasysearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
        return mapping;
    }
}
