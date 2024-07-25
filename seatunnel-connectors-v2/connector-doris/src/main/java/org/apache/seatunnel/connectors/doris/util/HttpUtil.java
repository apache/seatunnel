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

import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;

/** util to build http client. */
public class HttpUtil {
    private final HttpClientBuilder httpClientBuilder =
            HttpClients.custom()
                    .addInterceptorFirst(
                                (HttpRequestInterceptor)
                                        (request, context) -> {
                                            //If there is no data for the first time, TRANSFER_ENCODING will be added to the request header. Doris initiates a redirect to be and checks whether there is TRANSFER_ENCODING in the request header. If there is, it will be abnormal, so it needs to be removed.
                                            request.removeHeaders(HTTP.TRANSFER_ENCODING);
                                        })
                    .setRedirectStrategy(
                            new DefaultRedirectStrategy() {
                                @Override
                                protected boolean isRedirectable(String method) {
                                    return true;
                                }
                            });

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }
}
