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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesApplicationCli;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.ApiClient;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void retriesTransientPodListingFailures() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch listed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    if (requests.getAndIncrement() == 0) {
                                                        return new Response.Builder()
                                                                .request(chain.request())
                                                                .protocol(Protocol.HTTP_1_1)
                                                                .code(500)
                                                                .message("Internal Server Error")
                                                                .body(
                                                                        ResponseBody.create(
                                                                                "{\"message\":\"transient failure\"}",
                                                                                MediaType.get(
                                                                                        "application/json")))
                                                                .build();
                                                    }
                                                    return new Response.Builder()
                                                            .request(chain.request())
                                                            .protocol(Protocol.HTTP_1_1)
                                                            .code(200)
                                                            .message("OK")
                                                            .body(
                                                                    ResponseBody.create(
                                                                            "{\"apiVersion\":\"v1\",\"kind\":\"PodList\",\"items\":[]}",
                                                                            MediaType.get(
                                                                                    "application/json")))
                                                            .build();
                                                })
                                        .build());
        try (KubernetesClient api = new DefaultKubernetesClient(client, "test")) {
            KubernetesWatch watch =
                    api.watchPods("role=worker", 10, pods -> listed.countDown(), error::set);
            try {
                assertTrue(listed.await(5, TimeUnit.SECONDS));
                assertTrue(requests.get() >= 2);
                assertNull(error.get());
            } finally {
                watch.close();
            }
        }
    }

    @Test
    void reportsListenerFailureWithoutRetryingIt() throws Exception {
        CountDownLatch reported = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain ->
                                                        new Response.Builder()
                                                                .request(chain.request())
                                                                .protocol(Protocol.HTTP_1_1)
                                                                .code(200)
                                                                .message("OK")
                                                                .body(
                                                                        ResponseBody.create(
                                                                                "{\"apiVersion\":\"v1\",\"kind\":\"PodList\",\"items\":[]}",
                                                                                MediaType.get(
                                                                                        "application/json")))
                                                                .build())
                                        .build());
        try (KubernetesClient api = new DefaultKubernetesClient(client, "test")) {
            KubernetesWatch watch =
                    api.watchPods(
                            "role=worker",
                            10,
                            pods -> {
                                throw new IllegalStateException("listener failure");
                            },
                            failure -> {
                                error.set(failure);
                                reported.countDown();
                            });
            try {
                assertTrue(reported.await(5, TimeUnit.SECONDS));
                assertEquals("listener failure", error.get().getMessage());
            } finally {
                watch.close();
            }
        }
    }

    @Test
    void createsAccompanyingResourcesWithTheOwnerReference() throws Exception {
        List<String> requests = new ArrayList<>();
        AtomicReference<String> postedBody = new AtomicReference<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    requests.add(
                                                            chain.request().method()
                                                                    + " "
                                                                    + chain.request()
                                                                            .url()
                                                                            .encodedPath());
                                                    Buffer body = new Buffer();
                                                    chain.request().body().writeTo(body);
                                                    postedBody.set(body.readUtf8());
                                                    return new Response.Builder()
                                                            .request(chain.request())
                                                            .protocol(Protocol.HTTP_1_1)
                                                            .code(201)
                                                            .message("Created")
                                                            .body(
                                                                    ResponseBody.create(
                                                                            "{\"kind\":\"Secret\"}",
                                                                            MediaType.get(
                                                                                    "application/json")))
                                                            .build();
                                                })
                                        .build());
        KubernetesApplicationParameters parameters = parameters();
        KubernetesSecret secret = KubernetesResourceFactory.secret("app", parameters);
        KubernetesJob job =
                KubernetesResourceFactory.job(
                        "app", SeatunnelKubernetesApplicationCli.class.getName(), parameters);
        job.getInternalResource().getMetadata().setUid("owner-uid");
        try (KubernetesClient api = new DefaultKubernetesClient(client, "test")) {
            api.createAccompanyingResources(
                    job, Collections.<KubernetesResource<?>>singletonList(secret));
        }
        assertEquals(Collections.singletonList("POST /api/v1/namespaces/test/secrets"), requests);
        assertTrue(postedBody.get().contains("\"uid\":\"owner-uid\""));
    }

    @Test
    void deletesOnlyTheOwnerJobAndLabeledPods() throws Exception {
        List<String> requests = new ArrayList<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    requests.add(
                                                            chain.request().method()
                                                                    + " "
                                                                    + chain.request()
                                                                            .url()
                                                                            .encodedPath());
                                                    return new Response.Builder()
                                                            .request(chain.request())
                                                            .protocol(Protocol.HTTP_1_1)
                                                            .code(200)
                                                            .message("OK")
                                                            .body(
                                                                    ResponseBody.create(
                                                                            "{\"kind\":\"Status\"}",
                                                                            MediaType.get(
                                                                                    "application/json")))
                                                            .build();
                                                })
                                        .build());
        try (KubernetesClient api = new DefaultKubernetesClient(client, "test")) {
            api.deleteApplication("app");
        }
        assertEquals(2, requests.size());
        assertTrue(
                requests.get(0).startsWith("DELETE /apis/batch/v1/namespaces/test/jobs/app"),
                requests.toString());
        assertTrue(
                requests.get(1).startsWith("DELETE /api/v1/namespaces/test/pods"),
                requests.toString());
        assertFalse(
                requests.stream()
                        .anyMatch(
                                request ->
                                        request.contains("/secrets")
                                                || request.contains("/services")),
                requests.toString());
    }

    private static KubernetesApplicationParameters parameters() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        return KubernetesApplicationParameters.from(
                specification, ReadonlyConfig.fromMap(new HashMap<>(options)));
    }
}
