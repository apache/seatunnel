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
import org.apache.http.protocol.RequestContent;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/** util to build http client. */
public class HttpUtil {
    private final HttpClientBuilder httpClientBuilder =
            HttpClients.custom()
                    .setRedirectStrategy(
                            new DefaultRedirectStrategy() {
                                @Override
                                protected boolean isRedirectable(String method) {
                                    return true;
                                }
                            })
                    .addInterceptorLast(new RequestContent(true));

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
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
