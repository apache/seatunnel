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

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.informer.ListerWatcher;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ListMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.Watchable;

import java.net.ConnectException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

class InformerPodWatchTest {

    /**
     * The reflector retries a refused watch connection internally instead of calling the informer
     * exception handler, so the watch itself has to report the attempts: otherwise a permanently
     * unreachable API server would never exhaust the transient failure budget.
     */
    @Test
    void reportsWatchFailureWhenReconnectsExhaustTheTransientFailureBudget() throws Exception {
        CountDownLatch reported = new CountDownLatch(1);
        AtomicReference<Exception> reportedFailure = new AtomicReference<>();
        ListerWatcher<V1Pod, V1PodList> refusedConnections =
                new ListerWatcher<>() {
                    @Override
                    public V1PodList list(CallGeneratorParams params) {
                        return new V1PodList()
                                .metadata(new V1ListMeta().resourceVersion("1"))
                                .items(Collections.emptyList());
                    }

                    @Override
                    public Watchable<V1Pod> watch(CallGeneratorParams params) throws ApiException {
                        throw new ApiException(new ConnectException("Connection refused"));
                    }
                };
        try (InformerPodWatch watch =
                new InformerPodWatch(
                        watchClient(),
                        refusedConnections,
                        "app=test",
                        new PodWatchCallback() {
                            @Override
                            public void onPodAdded(KubernetesPod pod) {
                                // No worker pod is tracked before the connection is refused.
                            }

                            @Override
                            public void onPodModified(KubernetesPod pod) {
                                // No worker pod is tracked before the connection is refused.
                            }

                            @Override
                            public void onPodDeleted(KubernetesPod pod) {
                                // No worker pod is tracked before the connection is refused.
                            }

                            @Override
                            public void onWatchError(Exception failure) {
                                reportedFailure.set(failure);
                                reported.countDown();
                            }
                        },
                        200L)) {
            watch.start();
            assertTrue(
                    reported.await(30, TimeUnit.SECONDS),
                    "a refused watch connection must exhaust the transient failure budget");
            assertTrue(
                    reportedFailure.get().getMessage().contains("transient failure budget"),
                    reportedFailure.get().getMessage());
        }
    }

    /** @return client configured the way the informer requires: watches must not time out */
    private static ApiClient watchClient() {
        ApiClient client = new ApiClient();
        client.setReadTimeout(0);
        return client;
    }
}
