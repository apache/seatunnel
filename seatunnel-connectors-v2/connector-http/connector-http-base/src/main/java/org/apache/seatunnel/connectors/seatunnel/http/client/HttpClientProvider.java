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

import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpClientProvider implements AutoCloseable {
    private static final String ENCODING = "UTF-8";
    private static final String APPLICATION_JSON = "application/json";
    private static final int INITIAL_CAPACITY = 16;
    private RequestConfig requestConfig;
    private final CloseableHttpClient httpClient;
    private final Retryer<CloseableHttpResponse> retryer;

    public HttpClientProvider(HttpParameter httpParameter) {
        this.httpClient = HttpClients.createDefault();
        this.retryer = buildRetryer(httpParameter);
        this.requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(httpParameter.getConnectTimeoutMs())
                        .setSocketTimeout(httpParameter.getSocketTimeoutMs())
                        .build();
    }

    private Retryer<CloseableHttpResponse> buildRetryer(HttpParameter httpParameter) {
        if (httpParameter.getRetry() < 1) {
            return RetryerBuilder.<CloseableHttpResponse>newBuilder().build();
        }
        return RetryerBuilder.<CloseableHttpResponse>newBuilder()
                .retryIfException(ex -> ExceptionUtils.indexOfType(ex, IOException.class) != -1)
                .withStopStrategy(StopStrategies.stopAfterAttempt(httpParameter.getRetry()))
                .withWaitStrategy(
                        WaitStrategies.fibonacciWait(
                                httpParameter.getRetryBackoffMultiplierMillis(),
                                httpParameter.getRetryBackoffMaxMillis(),
                                TimeUnit.MILLISECONDS))
                .withRetryListener(
                        new RetryListener() {
                            @Override
                            public <V> void onRetry(Attempt<V> attempt) {
                                if (attempt.hasException()) {
                                    log.warn(
                                            String.format(
                                                    "[%d] request http failed",
                                                    attempt.getAttemptNumber()),
                                            attempt.getExceptionCause());
                                }
                            }
                        })
                .build();
    }

    public HttpResponse execute(
            String url,
            String method,
            Map<String, String> headers,
            Map<String, String> params,
            String body)
            throws Exception {
        // convert method option to uppercase
        method = method.toUpperCase(Locale.ROOT);
        if (HttpPost.METHOD_NAME.equals(method)) {
            return doPost(url, headers, params, body);
        }
        if (HttpGet.METHOD_NAME.equals(method)) {
            return doGet(url, headers, params);
        }
        if (HttpPut.METHOD_NAME.equals(method)) {
            return doPut(url, params);
        }
        if (HttpDelete.METHOD_NAME.equals(method)) {
            return doDelete(url, params);
        }
        // if http method that user assigned is not support by http provider, default do get
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
        return doGet(url, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Send a get request with request parameters
     *
     * @param url request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doGet(String url, Map<String, String> params) throws Exception {
        return doGet(url, Collections.emptyMap(), params);
    }

    /**
     * Send a get request with request headers and request parameters
     *
     * @param url request address
     * @param headers request header map
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doGet(String url, Map<String, String> headers, Map<String, String> params)
            throws Exception {
        // Create access address
        URIBuilder uriBuilder = new URIBuilder(url);
        // add parameter to uri
        addParameters(uriBuilder, params);
        // create a new http get
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        // set default request config
        httpGet.setConfig(requestConfig);
        // set request header
        addHeaders(httpGet, headers);
        // return http response
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
        return doPost(url, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Send post request with request parameters
     *
     * @param url request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url, Map<String, String> params) throws Exception {
        return doPost(url, Collections.emptyMap(), params);
    }

    /**
     * Send a post request with request headers and request parameters
     *
     * @param url request address
     * @param headers request header map
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url, Map<String, String> headers, Map<String, String> params)
            throws Exception {
        // create a new http get
        HttpPost httpPost = new HttpPost(url);
        // set default request config
        httpPost.setConfig(requestConfig);
        // set request header
        addHeaders(httpPost, headers);
        // set request params
        addParameters(httpPost, params);
        // return http response
        return getResponse(httpPost);
    }

    /**
     * Send a post request with request body and without headers
     *
     * @param url request address
     * @param body request body conetent
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url, String body) throws Exception {
        return doPost(url, Collections.emptyMap(), body);
    }

    /**
     * Send a post request with request headers and request body
     *
     * @param url request address
     * @param headers request header map
     * @param body request body content
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(String url, Map<String, String> headers, String body)
            throws Exception {
        // create a new http post
        HttpPost httpPost = new HttpPost(url);
        // set default request config
        httpPost.setConfig(requestConfig);
        // set request header
        addHeaders(httpPost, headers);
        // add body in request
        addBody(httpPost, body);
        // return http response
        return getResponse(httpPost);
    }

    /**
     * Send a post request with request headers and request body
     *
     * @param url request address
     * @param headers request header map
     * @param byteArrayEntity request snappy body content
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(
            String url, Map<String, String> headers, ByteArrayEntity byteArrayEntity)
            throws Exception {
        // create a new http post
        HttpPost httpPost = new HttpPost(url);
        // set default request config
        httpPost.setConfig(requestConfig);
        // set request header
        addHeaders(httpPost, headers);
        // add body in request
        httpPost.getRequestLine();
        httpPost.setEntity(byteArrayEntity);
        // return http response
        return getResponse(httpPost);
    }

    /**
     * Send a post request with request headers , request parameters and request body
     *
     * @param url request address
     * @param headers request header map
     * @param params request parameter map
     * @param body request body
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPost(
            String url, Map<String, String> headers, Map<String, String> params, String body)
            throws Exception {
        // create a new http get
        HttpPost httpPost = new HttpPost(url);
        // set default request config
        httpPost.setConfig(requestConfig);
        // set request header
        addHeaders(httpPost, headers);
        // set request params
        addParameters(httpPost, params);
        // add body in request
        addBody(httpPost, body);
        // return http response
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
        return doPut(url, Collections.emptyMap());
    }

    /**
     * Send a put request with request parameters
     *
     * @param url request address
     * @param params request parameter map
     * @return http response result
     * @throws Exception information
     */
    public HttpResponse doPut(String url, Map<String, String> params) throws Exception {
        // create a new http put
        HttpPut httpPut = new HttpPut(url);
        // set default request config
        httpPut.setConfig(requestConfig);
        // set request params
        addParameters(httpPut, params);
        // return http response
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
        // create a new http delete
        HttpDelete httpDelete = new HttpDelete(url);
        // set default request config
        httpDelete.setConfig(requestConfig);
        // return http response
        return getResponse(httpDelete);
    }

    /**
     * Send delete request with request parameters
     *
     * @param url request address
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

    private HttpResponse getResponse(HttpRequestBase request) throws Exception {
        // execute request
        try (CloseableHttpResponse httpResponse = retryWithException(request)) {
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

    private CloseableHttpResponse retryWithException(HttpRequestBase request) throws Exception {
        return retryer.call(() -> httpClient.execute(request));
    }

    private void addParameters(URIBuilder builder, Map<String, String> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return;
        }
        params.forEach(builder::setParameter);
    }

    private void addParameters(HttpEntityEnclosingRequestBase request, Map<String, String> params)
            throws UnsupportedEncodingException {
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
        headers.forEach(request::addHeader);
    }

    private boolean checkAlreadyHaveContentType(HttpEntityEnclosingRequestBase request) {
        if (request.getEntity() != null && request.getEntity().getContentType() != null) {
            return HTTP.CONTENT_TYPE.equals(request.getEntity().getContentType().getName());
        }
        return false;
    }

    private void addBody(HttpEntityEnclosingRequestBase request, String body) {
        if (checkAlreadyHaveContentType(request)) {
            return;
        }
        request.addHeader(HTTP.CONTENT_TYPE, APPLICATION_JSON);

        if (StringUtils.isBlank(body)) {
            body = "";
        }

        StringEntity entity = new StringEntity(body, ContentType.APPLICATION_JSON);
        entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, APPLICATION_JSON));
        request.setEntity(entity);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}
