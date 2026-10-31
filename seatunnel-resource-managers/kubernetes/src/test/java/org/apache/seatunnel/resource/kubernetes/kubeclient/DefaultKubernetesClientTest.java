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

import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            api.startJob("application");
        }
    }

    @Test
    void retriesTransientPodListingFailures() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch added = new CountDownLatch(1);
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
                                                    return podResponse(chain.request());
                                                })
                                        .build());
        // The pod informer requires a client without a read timeout.
        client.setReadTimeout(0);
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            KubernetesWatch watch = api.watchPods("role=worker", callback(added, error));
            try {
                assertTrue(added.await(5, TimeUnit.SECONDS));
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
                                        .addInterceptor(chain -> podResponse(chain.request()))
                                        .build());
        // The pod informer requires a client without a read timeout.
        client.setReadTimeout(0);
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            KubernetesWatch watch =
                    api.watchPods(
                            "role=worker",
                            new PodWatchCallback() {
                                @Override
                                public void onPodAdded(KubernetesPod pod) {
                                    throw new IllegalStateException("listener failure");
                                }

                                @Override
                                public void onPodModified(KubernetesPod pod) {}

                                @Override
                                public void onPodDeleted(KubernetesPod pod) {}

                                @Override
                                public void onWatchError(Exception failure) {
                                    error.set(failure);
                                    reported.countDown();
                                }
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
        KubernetesSecret secret =
                new KubernetesSecret(
                        new V1Secret()
                                .apiVersion("v1")
                                .kind("Secret")
                                .metadata(new V1ObjectMeta().name("app"))
                                .stringData(Collections.singletonMap("PASSWORD", "value")));
        KubernetesConfigMap configMap =
                new KubernetesConfigMap(
                        new V1ConfigMap()
                                .apiVersion("v1")
                                .kind("ConfigMap")
                                .metadata(new V1ObjectMeta().name("app-runtime"))
                                .data(Collections.singletonMap("seatunnel.yaml", "engine { }")));
        KubernetesJob job =
                new KubernetesJob(
                        new V1Job()
                                .apiVersion("batch/v1")
                                .kind("Job")
                                .metadata(new V1ObjectMeta().name("app")));
        KubernetesService service =
                new KubernetesService(
                        new V1Service()
                                .apiVersion("v1")
                                .kind("Service")
                                .metadata(new V1ObjectMeta().name("app-master"))
                                .spec(new V1ServiceSpec().clusterIP("None")));
        job.getInternalResource().getMetadata().setUid("owner-uid");
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            api.createAccompanyingResources(
                    job, Arrays.<KubernetesResource<?>>asList(secret, configMap, service));
        }
        assertEquals(
                Arrays.asList(
                        "POST /api/v1/namespaces/test/secrets",
                        "POST /api/v1/namespaces/test/configmaps",
                        "POST /api/v1/namespaces/test/services"),
                requests);
        assertTrue(postedBody.get().contains("\"uid\":\"owner-uid\""));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deletesOnlyTheOwnerJobAndLabeledPods(boolean missingOwner) throws Exception {
        List<String> requests = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        List<String> decodedQueries = new ArrayList<>();
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
                                                    queries.add(
                                                            chain.request().url().encodedQuery());
                                                    decodedQueries.add(
                                                            chain.request().url().query());
                                                    return new Response.Builder()
                                                            .request(chain.request())
                                                            .protocol(Protocol.HTTP_1_1)
                                                            .code(
                                                                    missingOwner
                                                                                    && chain.request()
                                                                                            .method()
                                                                                            .equals(
                                                                                                    "GET")
                                                                            ? 404
                                                                            : 200)
                                                            .message("OK")
                                                            .body(
                                                                    ResponseBody.create(
                                                                            "{\"kind\":\"Status\"}",
                                                                            MediaType.get(
                                                                                    "application/json")))
                                                            .build();
                                                })
                                        .build());
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            if (missingOwner) {
                new KubernetesApplicationClient(api, "app").cancel();
                assertEquals("GET /apis/batch/v1/namespaces/test/jobs/app", requests.remove(0));
                queries.remove(0);
                decodedQueries.remove(0);
            } else {
                api.deleteApplication("app");
            }
        }
        assertEquals(3, requests.size());
        assertTrue(
                requests.get(0).startsWith("DELETE /apis/batch/v1/namespaces/test/jobs/app"),
                requests.toString());
        assertTrue(
                requests.get(1).startsWith("DELETE /api/v1/namespaces/test/pods"),
                requests.toString());
        assertTrue(
                requests.get(2).startsWith("DELETE /api/v1/namespaces/test/pods"),
                requests.toString());
        // Workers are deleted immediately; the master keeps its own termination grace period.
        assertTrue(queries.get(0).contains("propagationPolicy=Foreground"), queries.toString());
        assertTrue(queries.get(1).contains("worker"), queries.toString());
        assertTrue(queries.get(1).contains("gracePeriodSeconds=0"), queries.toString());
        assertTrue(queries.get(1).contains("propagationPolicy=Background"), queries.toString());
        // Dropping the selector would delete every Pod in the namespace, so both deletions must
        // stay scoped to this application's role.
        assertTrue(
                decodedQueries
                        .get(1)
                        .contains(
                                "labelSelector="
                                        + KubernetesResourceIdentity.selector(
                                                "app", KubernetesConstants.WORKER_ROLE)),
                decodedQueries.toString());
        assertTrue(queries.get(2).contains("master"), queries.toString());
        assertFalse(queries.get(2).contains("gracePeriodSeconds"), queries.toString());
        assertTrue(
                decodedQueries
                        .get(2)
                        .contains(
                                "labelSelector="
                                        + KubernetesResourceIdentity.selector(
                                                "app", KubernetesConstants.MASTER_ROLE)),
                decodedQueries.toString());
        assertFalse(
                requests.stream()
                        .anyMatch(
                                request ->
                                        request.contains("/secrets")
                                                || request.contains("/services")),
                requests.toString());
    }

    @Test
    void haStatePatchTestsUidAndResourceVersionBeforeChangingAnnotations() throws Exception {
        AtomicReference<String> patch = new AtomicReference<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    assertEquals("PATCH", chain.request().method());
                                                    Buffer body = new Buffer();
                                                    chain.request().body().writeTo(body);
                                                    patch.set(body.readUtf8());
                                                    return jsonResponse(
                                                            chain.request(),
                                                            200,
                                                            "{\"kind\":\"Job\"}");
                                                })
                                        .build());
        KubernetesJob owner =
                new KubernetesJob(
                        new V1Job()
                                .metadata(
                                        new V1ObjectMeta()
                                                .name("app")
                                                .uid("original")
                                                .resourceVersion("17")));
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            api.updateHaState(
                    owner, Collections.singletonMap(HaApplicationState.INTENT, "requested"));
        }
        assertTrue(patch.get().contains("\"op\":\"test\""));
        assertTrue(patch.get().contains("/metadata/uid"));
        assertTrue(patch.get().contains("original"));
        assertTrue(patch.get().contains("/metadata/resourceVersion"));
        assertTrue(patch.get().contains("17"));
        assertTrue(
                patch.get().indexOf("/metadata/resourceVersion")
                        < patch.get().indexOf("/metadata/annotations"));
    }

    @Test
    void haCancelUsesUidPreconditionWithoutDeletingPodsByLabel() throws Exception {
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
                                                    Buffer body = new Buffer();
                                                    chain.request().body().writeTo(body);
                                                    V1DeleteOptions deletion =
                                                            new io.kubernetes.client.openapi.JSON()
                                                                    .deserialize(
                                                                            body.readUtf8(),
                                                                            V1DeleteOptions.class);
                                                    assertEquals(
                                                            "original",
                                                            deletion.getPreconditions().getUid());
                                                    assertEquals(
                                                            "Foreground",
                                                            deletion.getPropagationPolicy());
                                                    assertEquals(
                                                            "Foreground",
                                                            chain.request()
                                                                    .url()
                                                                    .queryParameter(
                                                                            "propagationPolicy"));
                                                    return jsonResponse(
                                                            chain.request(),
                                                            200,
                                                            "{\"kind\":\"Status\"}");
                                                })
                                        .build());
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            api.deleteApplication("app", "original");
        }
        assertEquals(
                Collections.singletonList("DELETE /apis/batch/v1/namespaces/test/jobs/app"),
                requests);
    }

    @Test
    void haCleanupPreservesUnownedResourcesAndContinuesPastConcurrentServiceDeletion()
            throws Exception {
        List<String> deleted = new ArrayList<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    String path =
                                                            chain.request().url().encodedPath();
                                                    if (chain.request().method().equals("DELETE")) {
                                                        deleted.add(path);
                                                        Buffer body = new Buffer();
                                                        chain.request().body().writeTo(body);
                                                        V1DeleteOptions deletion =
                                                                new io.kubernetes
                                                                                .client
                                                                                .openapi
                                                                                .JSON()
                                                                        .deserialize(
                                                                                body.readUtf8(),
                                                                                V1DeleteOptions
                                                                                        .class);
                                                        assertEquals(
                                                                "resource-uid",
                                                                deletion.getPreconditions()
                                                                        .getUid());
                                                        if (path.contains("/statefulsets/")
                                                                || path.contains("/pods/")) {
                                                            assertEquals(
                                                                    "Foreground",
                                                                    deletion
                                                                            .getPropagationPolicy());
                                                            assertEquals(
                                                                    0L,
                                                                    deletion
                                                                            .getGracePeriodSeconds());
                                                        } else {
                                                            assertEquals(
                                                                    "Background",
                                                                    deletion
                                                                            .getPropagationPolicy());
                                                        }
                                                        return jsonResponse(
                                                                chain.request(),
                                                                path.endsWith("app") ? 404 : 200,
                                                                "{\"kind\":\"Status\"}");
                                                    }
                                                    if (path.endsWith("/jobs/app")) {
                                                        return jsonResponse(
                                                                chain.request(),
                                                                200,
                                                                "{\"metadata\":{\"name\":\"app\",\"uid\":\"original\"}}");
                                                    }
                                                    if (path.endsWith("app-master")) {
                                                        return jsonResponse(
                                                                chain.request(),
                                                                200,
                                                                "{\"metadata\":"
                                                                        + ownedMetadata(
                                                                                "app-master")
                                                                        + "}");
                                                    }
                                                    if (path.endsWith("app-worker")) {
                                                        return jsonResponse(
                                                                chain.request(),
                                                                200,
                                                                "{\"metadata\":{\"name\":\"app-worker\",\"uid\":\"user-owned\"}}");
                                                    }
                                                    if (path.endsWith("/pods")) {
                                                        return jsonResponse(
                                                                chain.request(),
                                                                200,
                                                                "{\"items\":[{\"metadata\":"
                                                                        + ownedMetadata(
                                                                                "app-worker-0")
                                                                        + "},{\"metadata\":{\"name\":\"user-pod\",\"uid\":\"user-owned\"}}]}");
                                                    }
                                                    assertTrue(path.endsWith("/services"));
                                                    assertTrue(
                                                            chain.request()
                                                                    .url()
                                                                    .queryParameter("labelSelector")
                                                                    .contains("app"));
                                                    return jsonResponse(
                                                            chain.request(),
                                                            200,
                                                            "{\"items\":[{\"metadata\":"
                                                                    + ownedMetadata("app")
                                                                    + "},{\"metadata\":"
                                                                    + ownedMetadata("app-discovery")
                                                                    + "},{\"metadata\":{\"name\":\"user-service\",\"uid\":\"user-owned\"}}]}");
                                                })
                                        .build());
        KubernetesJob owner =
                new KubernetesJob(
                        new V1Job().metadata(new V1ObjectMeta().name("app").uid("original")));
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            api.deleteHaResources(owner);
        }
        assertEquals(
                Arrays.asList(
                        "/apis/apps/v1/namespaces/test/statefulsets/app-master",
                        "/api/v1/namespaces/test/pods/app-worker-0",
                        "/api/v1/namespaces/test/services/app",
                        "/api/v1/namespaces/test/services/app-discovery"),
                deleted);
    }

    @Test
    void haCleanupRefusesReusedOwnerNameBeforeAnyDeletion() throws Exception {
        AtomicInteger deletes = new AtomicInteger();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    if (chain.request().method().equals("DELETE")) {
                                                        deletes.incrementAndGet();
                                                    }
                                                    return jsonResponse(
                                                            chain.request(),
                                                            200,
                                                            "{\"metadata\":{\"name\":\"app\",\"uid\":\"replacement\"}}");
                                                })
                                        .build());
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            api.deleteHaResources(
                                    new KubernetesJob(
                                            new V1Job()
                                                    .metadata(
                                                            new V1ObjectMeta()
                                                                    .name("app")
                                                                    .uid("original")))));
        }
        assertEquals(0, deletes.get());
    }

    @Test
    void orphanCleanupPreservesReplacementOwnerAndPods() throws Exception {
        for (int appearsOnRead : new int[] {1, 2}) {
            AtomicInteger reads = new AtomicInteger();
            AtomicInteger deletes = new AtomicInteger();
            ApiClient client =
                    new ApiClient()
                            .setHttpClient(
                                    new OkHttpClient.Builder()
                                            .addInterceptor(
                                                    chain -> {
                                                        if (chain.request()
                                                                .method()
                                                                .equals("DELETE")) {
                                                            deletes.incrementAndGet();
                                                        }
                                                        if (chain.request()
                                                                .url()
                                                                .encodedPath()
                                                                .endsWith("/jobs/app")) {
                                                            return jsonResponse(
                                                                    chain.request(),
                                                                    reads.incrementAndGet()
                                                                                    >= appearsOnRead
                                                                            ? 200
                                                                            : 404,
                                                                    "{\"metadata\":{\"name\":\"app\",\"uid\":\"replacement\"}}");
                                                        }
                                                        return jsonResponse(
                                                                chain.request(),
                                                                200,
                                                                "{\"items\":[{\"metadata\":"
                                                                        + ownedMetadata(
                                                                                "app-worker-0")
                                                                        + "}]}");
                                                    })
                                            .build());
            try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
                api.deleteOrphanPods("app");
            }
            assertEquals(0, deletes.get());
        }
    }

    @Test
    void orphanCleanupDeletesOnlyObservedJobOwnedPodUid() throws Exception {
        List<String> deleted = new ArrayList<>();
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain -> {
                                                    String path =
                                                            chain.request().url().encodedPath();
                                                    if (chain.request().method().equals("DELETE")) {
                                                        deleted.add(path);
                                                        Buffer body = new Buffer();
                                                        chain.request().body().writeTo(body);
                                                        V1DeleteOptions options =
                                                                new io.kubernetes
                                                                                .client
                                                                                .openapi
                                                                                .JSON()
                                                                        .deserialize(
                                                                                body.readUtf8(),
                                                                                V1DeleteOptions
                                                                                        .class);
                                                        assertEquals(
                                                                "resource-uid",
                                                                options.getPreconditions()
                                                                        .getUid());
                                                        return jsonResponse(
                                                                chain.request(),
                                                                404,
                                                                "{\"kind\":\"Status\"}");
                                                    }
                                                    if (path.endsWith("/jobs/app")) {
                                                        return jsonResponse(
                                                                chain.request(),
                                                                404,
                                                                "{\"kind\":\"Status\"}");
                                                    }
                                                    return jsonResponse(
                                                            chain.request(),
                                                            200,
                                                            "{\"items\":[{\"metadata\":"
                                                                    + ownedMetadata("app-worker-0")
                                                                    + "},{\"metadata\":{\"name\":\"user-pod\",\"uid\":\"user-owned\"}}]}");
                                                })
                                        .build());
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            api.deleteOrphanPods("app");
        }
        assertEquals(
                java.util.Collections.singletonList("/api/v1/namespaces/test/pods/app-worker-0"),
                deleted);
    }

    private static String ownedMetadata(String name) {
        return "{\"name\":\""
                + name
                + "\",\"uid\":\"resource-uid\",\"ownerReferences\":["
                + "{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"name\":\"app\",\"uid\":\"original\"}]}";
    }

    private static Response jsonResponse(Request request, int code, String body) {
        return new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(code)
                .message("test")
                .body(ResponseBody.create(body, MediaType.get("application/json")))
                .build();
    }

    @Test
    void reportsSdkFailuresAsTheProviderException() {
        ApiClient client =
                new ApiClient()
                        .setHttpClient(
                                new OkHttpClient.Builder()
                                        .addInterceptor(
                                                chain ->
                                                        new Response.Builder()
                                                                .request(chain.request())
                                                                .protocol(Protocol.HTTP_1_1)
                                                                .code(403)
                                                                .message("Forbidden")
                                                                .body(
                                                                        ResponseBody.create(
                                                                                "{\"message\":\"denied\"}",
                                                                                MediaType.get(
                                                                                        "application/json")))
                                                                .build())
                                        .build());
        try (KubernetesHaClient api = new DefaultKubernetesClient(client, "test")) {
            KubernetesClientException failure =
                    assertThrows(KubernetesClientException.class, () -> api.deletePod("worker"));
            assertEquals(403, failure.getCode());
            assertTrue(failure.getCause() instanceof ApiException, failure.toString());
            // The Kubernetes Status body explains RBAC, quota and admission failures.
            assertTrue(failure.getMessage().contains("denied"), failure.getMessage());
        }
    }

    /**
     * Serves a PodList for list requests and one Added event for the informer's watch request. The
     * informer reads {@code metadata.resourceVersion} from the list, so the PodList carries one.
     */
    private static Response podResponse(Request request) {
        boolean watch = "true".equals(request.url().queryParameter("watch"));
        String body =
                watch
                        ? "{\"type\":\"ADDED\",\"object\":{\"apiVersion\":\"v1\",\"kind\":\"Pod\","
                                + "\"metadata\":{\"name\":\"app-worker-1\"},"
                                + "\"status\":{\"phase\":\"Running\"}}}\n"
                        : "{\"apiVersion\":\"v1\",\"kind\":\"PodList\",\"metadata\":"
                                + "{\"resourceVersion\":\"1\"},\"items\":[{\"apiVersion\":\"v1\","
                                + "\"kind\":\"Pod\",\"metadata\":{\"name\":\"app-worker-1\"},"
                                + "\"status\":{\"phase\":\"Running\"}}]}";
        return new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(body, MediaType.get("application/json")))
                .build();
    }

    /** Callback that counts the first added Pod and records the first watch error. */
    private static PodWatchCallback callback(
            CountDownLatch added, AtomicReference<Exception> error) {
        return new PodWatchCallback() {
            @Override
            public void onPodAdded(KubernetesPod pod) {
                added.countDown();
            }

            @Override
            public void onPodModified(KubernetesPod pod) {}

            @Override
            public void onPodDeleted(KubernetesPod pod) {}

            @Override
            public void onWatchError(Exception failure) {
                error.set(failure);
            }
        };
    }
}
