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

package org.apache.seatunnel.spark.http.source.util;

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
import java.util.Map.Entry;
import java.util.Set;

public class HttpClientUtils {

    private HttpClientUtils() {
        throw new IllegalStateException("Utility class");
    }

    private static final String ENCODING = "UTF-8";
    private static final int CONNECT_TIMEOUT = 6000 * 2;
    private static final int SOCKET_TIMEOUT = 6000 * 10;
    private static final int INITIAL_CAPACITY = 16;

    /**
     * Send a get request without request headers and request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public static HttpClientResult doGet(String url) throws Exception {
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
    public static HttpClientResult doGet(String url, Map<String, String> params) throws Exception {
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
    public static HttpClientResult doGet(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
        // Create access address
        URIBuilder uriBuilder = new URIBuilder(url);
        if (params != null) {
            Set<Entry<String, String>> entrySet = params.entrySet();
            for (Entry<String, String> entry : entrySet) {
                uriBuilder.setParameter(entry.getKey(), entry.getValue());
            }
        }

        /**
         * setConnectTimeout:Set the connection timeout, in milliseconds.
         * setSocketTimeout:The timeout period (ie response time) for requesting data acquisition, in milliseconds.
         * If an interface is accessed, and the data cannot be returned within a certain amount of time, the call is simply abandoned.
         */
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setConfig(requestConfig);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // set request header
            packageHeader(headers, httpGet);
            // Execute the request and get the response result
            return getHttpClientResult(httpClient, httpGet);
        }
    }

    /**
     * Send a post request without request headers and request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public static HttpClientResult doPost(String url) throws Exception {
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
    public static HttpClientResult doPost(String url, Map<String, String> params) throws Exception {
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
    public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        /**
         * setConnectTimeout:Set the connection timeout, in milliseconds.
         * setSocketTimeout:The timeout period (ie response time) for requesting data acquisition, in milliseconds.
         * If an interface is accessed, and the data cannot be returned within a certain amount of time, the call is simply abandoned.
         */
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpPost.setConfig(requestConfig);
        // set request header
        packageHeader(headers, httpPost);

        // Encapsulate request parameters
        packageParam(params, httpPost);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // Execute the request and get the response result
            return getHttpClientResult(httpClient, httpPost);
        }
    }

    /**
     * Send a put request without request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public static HttpClientResult doPut(String url) throws Exception {
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
    public static HttpClientResult doPut(String url, Map<String, String> params) throws Exception {

        HttpPut httpPut = new HttpPut(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpPut.setConfig(requestConfig);

        packageParam(params, httpPut);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            return getHttpClientResult(httpClient, httpPut);
        }
    }

    /**
     * Send delete request without request parameters
     *
     * @param url request address
     * @return http response result
     * @throws Exception information
     */
    public static HttpClientResult doDelete(String url) throws Exception {

        HttpDelete httpDelete = new HttpDelete(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpDelete.setConfig(requestConfig);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            return getHttpClientResult(httpClient, httpDelete);
        }
    }

    /**
     * Send delete request with request parameters
     *
     * @param url    request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public static HttpClientResult doDelete(String url, Map<String, String> params) throws Exception {
        if (params == null) {
            params = new HashMap<>(INITIAL_CAPACITY);
        }

        params.put("_method", "delete");
        return doPost(url, params);
    }

    /**
     * encapsulate request header
     *
     * @param params     request header map
     * @param httpMethod http request method
     */
    public static void packageHeader(Map<String, String> params, HttpRequestBase httpMethod) {
        // encapsulate request header
        if (params != null) {
            Set<Entry<String, String>> entrySet = params.entrySet();
            for (Entry<String, String> entry : entrySet) {
                // Set to the request header to the HttpRequestBase object
                httpMethod.setHeader(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Encapsulate request parameters
     *
     * @param params     request parameter map
     * @param httpMethod http request method
     * @throws UnsupportedEncodingException exception information
     */
    public static void packageParam(Map<String, String> params, HttpEntityEnclosingRequestBase httpMethod) throws UnsupportedEncodingException {
        // Encapsulate request parameters
        if (params != null) {
            List<NameValuePair> nvps = new ArrayList<>();
            Set<Entry<String, String>> entrySet = params.entrySet();
            for (Entry<String, String> entry : entrySet) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }

            // Set to the request's http object
            httpMethod.setEntity(new UrlEncodedFormEntity(nvps, ENCODING));
        }
    }

    /**
     * get response result
     *
     * @param httpClient http client object
     * @param httpMethod http method onject
     * @return http response result
     * @throws Exception information
     */
    public static HttpClientResult getHttpClientResult(CloseableHttpClient httpClient, HttpRequestBase httpMethod) throws Exception {
        // execute request
        try (CloseableHttpResponse httpResponse = httpClient.execute(httpMethod)) {
            // get return result
            if (httpResponse != null && httpResponse.getStatusLine() != null) {
                String content = "";
                if (httpResponse.getEntity() != null) {
                    content = EntityUtils.toString(httpResponse.getEntity(), ENCODING);
                }
                return new HttpClientResult(httpResponse.getStatusLine().getStatusCode(), content);
            }
        }
        return new HttpClientResult(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    /**
     * release resources
     *
     * @param httpResponse http response object
     * @param httpClient   http client objet
     * @throws IOException information
     */
    public static void release(CloseableHttpResponse httpResponse, CloseableHttpClient httpClient) throws IOException {
        // release resources
        if (httpResponse != null) {
            httpResponse.close();
        }
        if (httpClient != null) {
            httpClient.close();
        }
    }

}
