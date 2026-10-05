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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/** util to build http client. */
public class HttpUtil {
    // Stream load upload sends "Expect: 100-continue" and relies on FE's 307 redirect to reach
    // BE. The default waitForContinue timeout of HttpRequestExecutor is only 3s: when FE is busy
    // (heavy load / FullGC), the client stops waiting, sends the non-repeatable request body to
    // FE, and can no longer follow the late 307 redirect. Wait 60s for upload traffic instead,
    // aligned with doris-flink-connector. Only the stream-load upload client needs this - the
    // empty-entity control requests (commit / abort / pre-commit) keep the default, so a slow FE
    // does not stretch checkpoint completion / transaction cleanup.
    private static final int WAIT_FOR_CONTINUE_TIMEOUT_MS = 60 * 1000;

    private static final DefaultRedirectStrategy REDIRECT_STRATEGY =
            new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            };

    private final HttpClientBuilder httpClientBuilder =
            HttpClients.custom()
                    .setRedirectStrategy(REDIRECT_STRATEGY)
                    .addInterceptorLast(new RequestContent(true));

    // Separate builder for the stream-load upload path, which sends a non-repeatable body and
    // needs a longer waitForContinue window than the default 3s.
    private final HttpClientBuilder streamLoadHttpClientBuilder =
            HttpClients.custom()
                    .setRequestExecutor(new HttpRequestExecutor(WAIT_FOR_CONTINUE_TIMEOUT_MS))
                    .setRedirectStrategy(REDIRECT_STRATEGY)
                    .addInterceptorLast(new RequestContent(true));

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }

    /**
     * Client for the stream-load upload path. Raises the waitForContinue timeout so a slow FE 307
     * redirect does not force the non-repeatable request body to be sent to FE before the redirect
     * arrives.
     */
    public CloseableHttpClient getStreamLoadHttpClient() {
        return streamLoadHttpClientBuilder.build();
    }

    public static CloseableHttpResponse executeWithRedirectTracking(
            CloseableHttpClient httpClient,
            HttpUriRequest request,
            String requestUrl,
            boolean directToBe,
            boolean enable2PC,
            String requestStage)
            throws IOException {
        HttpClientContext context = HttpClientContext.create();
        try {
            return httpClient.execute(request, context);
        } catch (IOException e) {
            String redirectLocation = resolveLastRedirectLocation(context);
            if (redirectLocation != null) {
                throw new DorisConnectorException(
                        DorisConnectorErrorCode.STREAM_LOAD_FAILED,
                        DorisRedirectExceptionBuilder.buildFollowUpFailure(
                                requestUrl,
                                redirectLocation,
                                directToBe,
                                enable2PC,
                                requestStage,
                                e.getMessage()),
                        e);
            }
            throw e;
        }
    }

    private static String resolveLastRedirectLocation(HttpClientContext context) {
        List<URI> redirectLocations = context.getRedirectLocations();
        if (redirectLocations == null || redirectLocations.isEmpty()) {
            return null;
        }
        return redirectLocations.get(redirectLocations.size() - 1).toString();
    }
}
