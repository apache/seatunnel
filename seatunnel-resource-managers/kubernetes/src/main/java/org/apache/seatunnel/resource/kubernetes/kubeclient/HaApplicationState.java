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

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Non-secret lifecycle metadata on the durable owner Job, protected by namespace RBAC. */
@lombok.extern.slf4j.Slf4j
public final class HaApplicationState {
    public static final String ENABLED = "seatunnel.apache.org/ha";
    public static final String INTENT = "seatunnel.apache.org/submission";
    public static final String JOB_ID = "seatunnel.apache.org/job-id";
    public static final String SPEC_DIGEST = "seatunnel.apache.org/spec-digest";
    public static final String STATUS = "seatunnel.apache.org/application-status";
    private static final String NEW = "new";
    private static final String REQUESTED = "requested";

    private HaApplicationState() {}

    public static Map<String, String> annotations(KubernetesJob job) {
        Map<String, String> annotations = job.getInternalResource().getMetadata().getAnnotations();
        return annotations == null ? java.util.Collections.emptyMap() : annotations;
    }

    public static boolean enabled(KubernetesJob job) {
        return "true".equals(annotations(job).get(ENABLED));
    }

    public static boolean unsubmitted(KubernetesJob owner) {
        return NEW.equals(annotations(owner).get(INTENT));
    }

    public static ApplicationStatus terminal(KubernetesJob job) {
        if (!enabled(job)) {
            return null;
        }
        String value = annotations(job).get(STATUS);
        for (ApplicationStatus status :
                new ApplicationStatus[] {
                    ApplicationStatus.SUCCEEDED,
                    ApplicationStatus.FAILED,
                    ApplicationStatus.CANCELED
                }) {
            if (status.name().equals(value)) {
                return status;
            }
        }
        return null;
    }

    public static String digest(String specification) {
        try {
            byte[] bytes =
                    MessageDigest.getInstance("SHA-256")
                            .digest(specification.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            for (byte b : bytes) {
                result.append(String.format("%02x", b & 255));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
    /** Only the successful resource-version CAS may send the first native execute request. */
    public static boolean claimSubmission(KubernetesHaClient api, KubernetesJob owner) {
        if (terminal(owner) != null || !unsubmitted(owner)) {
            return false;
        }
        Map<String, String> claimed = new HashMap<>(HaApplicationState.annotations(owner));
        claimed.put(HaApplicationState.INTENT, REQUESTED);
        try {
            api.updateHaState(owner, claimed);
            return true;
        } catch (KubernetesClientException conflict) {
            if (conflict.getCode() == 409 || conflict.getCode() == 422) {
                return false;
            }
            throw conflict;
        }
    }

    public static KubernetesJob validated(
            KubernetesJob owner, String uid, String encoded, long jobId) {
        return validatedDigest(owner, uid, digest(encoded), jobId);
    }

    public static KubernetesJob validatedDigest(
            KubernetesJob owner, String uid, String expectedDigest, long jobId) {
        if (uid == null
                || !uid.equals(owner.getUid())
                || owner.getInternalResource().getMetadata().getDeletionTimestamp() != null
                || !HaApplicationState.enabled(owner)
                || !Long.toString(jobId)
                        .equals(
                                HaApplicationState.annotations(owner)
                                        .get(HaApplicationState.JOB_ID))
                || !expectedDigest.equals(
                        HaApplicationState.annotations(owner)
                                .get(HaApplicationState.SPEC_DIGEST))) {
            throw new IllegalStateException(
                    "Application owner UID or immutable specification changed");
        }
        return owner;
    }

    /**
     * Records an immutable outcome with optimistic concurrency; another observer's outcome wins.
     */
    public static void recordTerminal(
            KubernetesHaClient api, KubernetesJob owner, ApplicationStatus result) {
        recordStatus(api, owner, result, false, () -> true);
    }

    public static void recordTerminal(
            KubernetesHaClient api,
            KubernetesJob owner,
            ApplicationStatus result,
            BooleanSupplier current) {
        recordStatus(api, owner, result, false, current);
    }

    public static void recordRunning(KubernetesHaClient api, KubernetesJob owner) {
        recordStatus(api, owner, ApplicationStatus.RUNNING, false, () -> true);
    }

    /** A startup timeout cannot supersede another observer's durable native RUNNING result. */
    public static boolean recordStartupFailure(KubernetesHaClient api, KubernetesJob owner) {
        return recordStatus(api, owner, ApplicationStatus.FAILED, true, () -> true);
    }

    private static boolean recordStatus(
            KubernetesHaClient api,
            KubernetesJob owner,
            ApplicationStatus result,
            boolean startupOnly,
            BooleanSupplier current) {
        for (int attempt = 0; attempt < 10; attempt++) {
            if (!current.getAsBoolean()) {
                return false;
            }
            if (HaApplicationState.terminal(owner) != null) {
                return true;
            }
            if (startupOnly
                    && ApplicationStatus.RUNNING
                            .name()
                            .equals(
                                    HaApplicationState.annotations(owner)
                                            .get(HaApplicationState.STATUS))) {
                return false;
            }
            Map<String, String> state = new HashMap<>(HaApplicationState.annotations(owner));
            state.put(HaApplicationState.STATUS, result.name());
            try {
                api.updateHaState(owner, state);
                return true;
            } catch (KubernetesClientException conflict) {
                if (conflict.getCode() != 409 && conflict.getCode() != 422) {
                    throw conflict;
                }
                KubernetesJob refreshed = api.getJob(owner.getName());
                if (!owner.getUid().equals(refreshed.getUid())
                        || refreshed.getInternalResource().getMetadata().getDeletionTimestamp()
                                != null
                        || !HaApplicationState.enabled(refreshed)
                        || !java.util.Objects.equals(
                                HaApplicationState.annotations(owner)
                                        .get(HaApplicationState.JOB_ID),
                                HaApplicationState.annotations(refreshed)
                                        .get(HaApplicationState.JOB_ID))
                        || !HaApplicationState.annotations(owner)
                                .get(HaApplicationState.SPEC_DIGEST)
                                .equals(
                                        HaApplicationState.annotations(refreshed)
                                                .get(HaApplicationState.SPEC_DIGEST))) {
                    throw new IllegalStateException(
                            "Application identity changed while recording result");
                }
                owner = refreshed;
            }
        }
        throw new IllegalStateException(
                "Could not record application outcome after concurrent updates");
    }

    public static void finalizeResources(KubernetesHaClient api, KubernetesJob owner)
            throws Exception {
        api.deleteHaResources(owner);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!api.listPods(KubernetesResourceIdentity.selector(owner.getName(), "master"))
                        .isEmpty()
                || !api.listPods(KubernetesResourceIdentity.selector(owner.getName(), "worker"))
                        .isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "HA compute cleanup is incomplete; owner must retry");
            }
            TimeUnit.SECONDS.sleep(1);
        }
        log.info(
                "HA application {} terminal {}: compute released; Job retention owns remaining configuration cleanup",
                owner.getName(),
                HaApplicationState.terminal(owner));
    }

    public static ApplicationStatus status(KubernetesJob job) {
        try {
            return ApplicationStatus.valueOf(annotations(job).getOrDefault(STATUS, "UNKNOWN"));
        } catch (IllegalArgumentException invalid) {
            return ApplicationStatus.UNKNOWN;
        }
    }

    public static Map<String, String> initial(String encoded, long jobId) {
        Map<String, String> state = new HashMap<>();
        state.put(ENABLED, "true");
        state.put(INTENT, NEW);
        state.put(JOB_ID, Long.toString(jobId));
        state.put(SPEC_DIGEST, digest(encoded));
        state.put(STATUS, ApplicationStatus.DEPLOYING.name());
        return state;
    }
}
