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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.ApiClient;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultKubernetesClientTest {
    @Test
    void startsJobWithFieldPatchWithoutControllerResourceVersion() throws Exception {
        ApiClient client = new ApiClient();
        client.setHttpClient(
                new OkHttpClient.Builder()
                        .addInterceptor(
                                chain -> {
                                    assertEquals("PATCH", chain.request().method());
                                    assertEquals(
                                            "/apis/batch/v1/namespaces/test/jobs/application",
                                            chain.request().url().encodedPath());
                                    assertEquals(
                                            "application/json-patch+json",
                                            chain.request().header("Content-Type"));
                                    Buffer body = new Buffer();
                                    chain.request().body().writeTo(body);
                                    assertEquals(
                                            "[{\"op\":\"replace\",\"path\":\"/spec/suspend\",\"value\":false}]",
                                            body.readUtf8());
                                    return new Response.Builder()
                                            .request(chain.request())
                                            .protocol(Protocol.HTTP_1_1)
                                            .code(200)
                                            .message("OK")
                                            .body(
                                                    ResponseBody.create(
                                                            "{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\"}",
                                                            MediaType.get("application/json")))
                                            .build();
                                })
                        .build());
        try (KubernetesClient api = new DefaultKubernetesClient(client, "test")) {
            api.startJob("application");
        }
    }
}
