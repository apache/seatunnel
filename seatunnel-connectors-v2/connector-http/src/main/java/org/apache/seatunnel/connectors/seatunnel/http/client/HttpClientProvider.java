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

package org.apache.seatunnel.connectors.seatunnel.http.client;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HttpClientProvider implements AutoCloseable {
    private final CloseableHttpClient httpClient;
    private static final String ENCODING = "UTF-8";
    private static final int CONNECT_TIMEOUT = 6000 * 2;
    private static final int SOCKET_TIMEOUT = 6000 * 10;
    private static final int INITIAL_CAPACITY = 16;

    private HttpClientProvider() {
        httpClient = HttpClients.createDefault();
    }

    public static HttpClientProvider getInstance() {
        return Singleton.INSTANCE;
    }

    public HttpResponse execute(String url, String method, Map<String, String> headers, Map<String, String> params) throws Exception {
        if ("POST".equals(method)) {
            return doPost(url, headers, params);
        }
        return doGet(url, headers, params);
    }

    /**
     * Send a get request without request headers and request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doGet(String url) throws Exception {
        return doGet(url, null, null);
    }

    /**
     * Send a get request with request parameters
     *
     * @param url    request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doGet(String url, Map<String, String> params) throws Exception {
        return doGet(url, null, params);
    }

    /**
     * Send a get request with request headers and request parameters
     *
     * @param url     request address
     * @param headers request header map
     * @param params  request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doGet(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
        // Create access address
        URIBuilder uriBuilder = new URIBuilder(url);
        addParameters(uriBuilder, params);

        /**
         * setConnectTimeout:Set the connection timeout, in milliseconds.
         * setSocketTimeout:The timeout period (ie response time) for requesting data acquisition, in milliseconds.
         * If an interface is accessed, and the data cannot be returned within a certain amount of time, the call is simply abandoned.
         */
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setConfig(requestConfig);

        addHeaders(httpGet, headers);
        return getResponse(httpGet);
    }

    /**
     * Send a post request without request headers and request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url) throws Exception {
        return doPost(url, null, null);
    }

    /**
     * Send post request with request parameters
     *
     * @param url    request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url, Map<String, String> params) throws Exception {
        return doPost(url, null, params);
    }

    /**
     * Send a post request with request headers and request parameters
     *
     * @param url     request address
     * @param headers request header map
     * @param params  request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        /**
         * setConnectTimeout:Set the connection timeout, in milliseconds.
         * setSocketTimeout:The timeout period (ie response time) for requesting data acquisition, in milliseconds.
         * If an interface is accessed, and the data cannot be returned within a certain amount of time, the call is simply abandoned.
         */
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpPost.setConfig(requestConfig);
        // set request header
        addHeaders(httpPost, headers);

        // Encapsulate request parameters
        addParameters(httpPost, params);
        return getResponse(httpPost);
    }

    /**
     * Send a put request without request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPut(String url) throws Exception {
        return doPut(url, null);
    }

    /**
     * Send a put request with request parameters
     *
     * @param url    request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPut(String url, Map<String, String> params) throws Exception {

        HttpPut httpPut = new HttpPut(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpPut.setConfig(requestConfig);

        addParameters(httpPut, params);
        return getResponse(httpPut);
    }

    /**
     * Send delete request without request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doDelete(String url) throws Exception {

        HttpDelete httpDelete = new HttpDelete(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpDelete.setConfig(requestConfig);
        return getResponse(httpDelete);
    }

    /**
     * Send delete request with request parameters
     *
     * @param url    request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doDelete(String url, Map<String, String> params) throws Exception {
        if (params == null) {
            params = new HashMap<>(INITIAL_CAPACITY);
        }

        params.put("_method", "delete");
        return doPost(url, params);
    }

    private HttpResponse getResponse(HttpRequestBase request) throws IOException {
        // execute request
        try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
            // get return result
            if (httpResponse != null && httpResponse.getStatusLine() != null) {
                String content = "";
                if (httpResponse.getEntity() != null) {
                    content = EntityUtils.toString(httpResponse.getEntity(), ENCODING);
                }
                return new HttpResponse(httpResponse.getStatusLine().getStatusCode(), content);
            }
        }
        return new HttpResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    private void addParameters(URIBuilder builder, Map<String, String> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return;
        }
        params.forEach((k, v) -> builder.setParameter(k, v));
    }

    private void addParameters(HttpEntityEnclosingRequestBase request, Map<String, String> params) throws UnsupportedEncodingException {
        if (Objects.isNull(params) || params.isEmpty()) {
            return;
        }
        List<NameValuePair> parameters = new ArrayList<>();
        Set<Map.Entry<String, String>> entrySet = params.entrySet();
        for (Map.Entry<String, String> e : entrySet) {
            String name = e.getKey();
            String value = e.getValue();
            NameValuePair pair = new BasicNameValuePair(name, value);
            parameters.add(pair);
        }
        // Set to the request's http object
        request.setEntity(new UrlEncodedFormEntity(parameters, ENCODING));
    }

    private void addHeaders(HttpRequestBase request, Map<String, String> headers) {
        if (Objects.isNull(headers) || headers.isEmpty()) {
            return;
        }
        headers.forEach((k, v) -> request.addHeader(k, v));
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    private static class Singleton {
        private static final HttpClientProvider INSTANCE = new HttpClientProvider();
    }
}
