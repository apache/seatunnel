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

package org.apache.seatunnel.connectors.elasticsearch.client;

import org.apache.seatunnel.connectors.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.elasticsearch.exception.BulkElasticsearchException;
import org.apache.seatunnel.connectors.elasticsearch.exception.GetElasticsearchVersionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.util.List;

public class EsRestClient {

    private static EsRestClient ES_REST_CLIENT;
    private static RestClient REST_CLIENT;

    private EsRestClient() {

    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private static RestClientBuilder getRestClientBuilder(List<String> hosts, String username, String password) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            String[] hostInfo = hosts.get(i).replace("http://", "").split(":");
            httpHosts[i] = new HttpHost(hostInfo[0], Integer.parseInt(hostInfo[1]));
        }

        RestClientBuilder builder = RestClient.builder(httpHosts)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectionRequestTimeout(10 * 1000)
                        .setSocketTimeout(5 * 60 * 1000));

        if (StringUtils.isNotEmpty(username)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return builder;
    }

    public static EsRestClient getInstance(List<String> hosts, String username, String password) {
        if (REST_CLIENT == null) {
            RestClientBuilder restClientBuilder = getRestClientBuilder(hosts, username, password);
            REST_CLIENT = restClientBuilder.build();
            ES_REST_CLIENT = new EsRestClient();
        }
        return ES_REST_CLIENT;
    }

    public BulkResponse bulk(String requestBody) {
        Request request = new Request("POST", "_bulk");
        request.setJsonEntity(requestBody);
        try {
            Response response = REST_CLIENT.performRequest(request);
            if (response == null) {
                throw new BulkElasticsearchException("bulk es Response is null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                ObjectMapper objectMapper = new ObjectMapper();
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = objectMapper.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new BulkElasticsearchException(String.format("bulk es response status code=%d,request boy=%s", response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new BulkElasticsearchException(String.format("bulk es error,request boy=%s", requestBody), e);
        }
    }

    /**
     * @return version.number, example:2.0.0
     */
    public static String getClusterVersion() {
        Request request = new Request("GET", "/");
        try {
            Response response = REST_CLIENT.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(result);
            JsonNode versionNode = jsonNode.get("version");
            return versionNode.get("number").asText();
        } catch (IOException e) {
            throw new GetElasticsearchVersionException("fail to get elasticsearch version.", e);
        }
    }

    public void close() throws IOException {
        REST_CLIENT.close();
    }

}
