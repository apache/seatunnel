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

package org.apache.seatunnel.connectors.seatunnel.splunk.client;

import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Minimal client for the Splunk HTTP Event Collector.
 *
 * <p>This connector does not reuse {@code connector-http-base}'s {@code HttpClientProvider} because
 * that provider always builds a default client and offers no hook for TLS configuration. Splunk
 * deployments commonly expose the collector behind the self-signed certificate created at install
 * time, so configurable certificate and hostname verification is a requirement here.
 */
@Slf4j
public class SplunkHecClient implements Closeable {

    private static final String AUTHORIZATION_PREFIX = "Splunk ";

    /** HTTP status returned by Splunk when the collector is saturated. */
    private static final int STATUS_TOO_MANY_REQUESTS = 429;

    private final String endpoint;
    private final String authorizationHeader;
    private final CloseableHttpClient httpClient;

    public SplunkHecClient(SplunkSinkConfig config) {
        this.endpoint = config.getEndpoint();
        this.authorizationHeader = AUTHORIZATION_PREFIX + config.getToken();
        this.httpClient = buildHttpClient(config);
    }

    /**
     * Sends one batch of newline-delimited HEC event envelopes.
     *
     * @param body concatenated event envelopes
     * @throws SplunkHecRetryableException when the failure is worth retrying (transport error, 429
     *     or 5xx)
     * @throws SplunkConnectorException when the collector rejected the batch permanently
     */
    public void send(String body) throws SplunkHecRetryableException {
        HttpPost httpPost = new HttpPost(endpoint);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authorizationHeader);
        httpPost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));

        int statusCode;
        String responseBody;
        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            statusCode = response.getStatusLine().getStatusCode();
            responseBody =
                    response.getEntity() == null
                            ? ""
                            : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SplunkHecRetryableException(
                    String.format("Failed to reach the Splunk HEC endpoint '%s'", endpoint), e);
        }

        if (statusCode == HttpStatus.SC_OK) {
            return;
        }

        String message =
                String.format(
                        "Splunk HEC endpoint '%s' rejected the batch with HTTP status %d, response: %s",
                        endpoint, statusCode, responseBody);
        if (isRetryable(statusCode)) {
            throw new SplunkHecRetryableException(message, null);
        }
        throw new SplunkConnectorException(SplunkConnectorErrorCode.SEND_EVENTS_FAILED, message);
    }

    /**
     * A 429 means the collector queue is full and a 5xx means the indexer is unhealthy; both clear
     * on their own. Every other status reflects a bad token, a bad index or a malformed payload,
     * none of which a retry can fix.
     */
    private static boolean isRetryable(int statusCode) {
        return statusCode == STATUS_TOO_MANY_REQUESTS
                || statusCode >= HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }

    private static CloseableHttpClient buildHttpClient(SplunkSinkConfig config) {
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(config.getConnectTimeoutMs())
                        .setSocketTimeout(config.getSocketTimeoutMs())
                        .setConnectionRequestTimeout(config.getConnectTimeoutMs())
                        .build();

        HttpClientBuilder builder =
                HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .disableAutomaticRetries();

        if (!config.isTlsVerifyCertificate()) {
            try {
                SSLContext sslContext =
                        SSLContexts.custom().loadTrustMaterial(new TrustAllStrategy()).build();
                builder.setSSLContext(sslContext);
            } catch (Exception e) {
                throw new SplunkConnectorException(
                        SplunkConnectorErrorCode.SSL_CONTEXT_FAILED,
                        "Failed to build a trust-all TLS context for the Splunk HEC client",
                        e);
            }
            log.warn(
                    "Splunk sink TLS certificate verification is disabled - not recommended for production");
        }
        if (!config.isTlsVerifyHostname()) {
            builder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
            log.warn(
                    "Splunk sink TLS hostname verification is disabled - not recommended for production");
        }
        return builder.build();
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    /** Signals a collector failure that is worth another attempt. */
    public static class SplunkHecRetryableException extends Exception {

        private static final long serialVersionUID = 1L;

        public SplunkHecRetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
