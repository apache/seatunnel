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

package org.apache.seatunnel.resource.kubernetes.cli;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.HaApplicationFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import com.hazelcast.client.config.ClientConfig;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HA members stay alive; a retryable owner observes one native job without electing or recovering
 * it.
 */
@Slf4j
public final class SeatunnelKubernetesHaApplication {
    private SeatunnelKubernetesHaApplication() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !("master".equals(args[1]) || "owner".equals(args[1]))) {
            throw new IllegalArgumentException("Expected <application-id> <master|owner>");
        }
        String encoded = System.getenv(KubernetesConstants.SPECIFICATION_ENV);
        KubernetesApplicationConfig parameters = KubernetesApplicationConfig.read(encoded);
        if (!parameters.isHaEnabled()) {
            throw new IllegalArgumentException("HA entrypoint requires HA configuration");
        }
        if ("master".equals(args[1])) {
            startMaster(args[0], parameters);
        } else {
            try (KubernetesClient api =
                    KubernetesClientFactory.create(
                            Collections.singletonMap(
                                    KubernetesOptions.NAMESPACE.key(), parameters.getNamespace()),
                            true)) {
                observe(
                        api,
                        args[0],
                        System.getenv(HaApplicationFactory.OWNER_UID_ENV),
                        encoded,
                        parameters);
            }
        }
    }

    private static void startMaster(String id, KubernetesApplicationConfig parameters)
            throws Exception {

        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        SeatunnelApplicationConfig.configureHa(
                config,
                SeatunnelApplicationConfig.clusterName(id),
                HaApplicationFactory.masterAddresses(id, parameters),
                false,
                parameters.getSpecification().getWorkerSpecification().getSlots(),
                parameters.getHaBackupCount());
        SeatunnelApplicationConfig.configureCheckpointRetention(config);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .setPort(parameters.getSpecification().getMasterPort())
                .setPortAutoIncrement(false);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        SeaTunnelServerStarter.createHazelcastInstance(
                config,
                null,
                ResourceManagerFactory.forExternallyManagedApplication(
                        parameters.getSpecification().getWorkerCount()));
        log.info("HA master online for application {}", id);
    }

    /**
     * Replays terminal results before constructing a client; only the intent CAS winner may submit.
     */
    static void observe(
            KubernetesClient api,
            String id,
            String uid,
            String encoded,
            KubernetesApplicationConfig parameters)
            throws Exception {

        ApplicationSpecification specification = parameters.getSpecification();
        KubernetesJob owner = validated(api.getJob(id), uid, encoded, specification.getJobId());
        if (HaApplicationState.terminal(owner) != null) {
            finalizeResources(api, owner);
            return;
        }
        ClientConfig config = ConfigProvider.locateAndGetClientConfig();
        config.setClusterName(SeatunnelApplicationConfig.clusterName(id));
        config.getNetworkConfig()
                .setAddresses(
                        Collections.singletonList(
                                id
                                        + "."
                                        + parameters.getNamespace()
                                        + ".svc:"
                                        + specification.getMasterPort()))
                .setSmartRouting(false);
        config.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(specification.getStartupTimeoutMillis());
        Common.setDeployMode(DeployMode.CLUSTER);
        Path jobFile = Files.createTempFile("seatunnel-application-", ".conf");
        try (SeaTunnelClient client = new SeaTunnelClient(config)) {
            Files.write(jobFile, specification.getJobConfig().getBytes(StandardCharsets.UTF_8));
            owner = validated(api.getJob(id), uid, encoded, specification.getJobId());
            ClientJobProxy proxy = client.getJobClient().getJobProxy(specification.getJobId());
            if ("new"
                    .equals(HaApplicationState.annotations(owner).get(HaApplicationState.INTENT))) {
                // Establish the requested redundancy before the first native IMap/job write.
                // Replacement observers attach directly; they never wait for the failed master.
                long joinDeadline =
                        System.nanoTime()
                                + TimeUnit.MILLISECONDS.toNanos(
                                        specification.getStartupTimeoutMillis());
                while (client.getMasterCount() < parameters.getMasterReplicas()) {
                    owner = validated(api.getJob(id), uid, encoded, specification.getJobId());
                    if (!"new"
                                    .equals(
                                            HaApplicationState.annotations(owner)
                                                    .get(HaApplicationState.INTENT))
                            || HaApplicationState.terminal(owner) != null) {
                        break;
                    }
                    if (System.nanoTime() > joinDeadline) {
                        if (recordStartupFailure(api, owner)) {
                            finalizeResources(
                                    api,
                                    validated(
                                            api.getJob(id),
                                            uid,
                                            encoded,
                                            specification.getJobId()));
                            return;
                        }
                        owner = validated(api.getJob(id), uid, encoded, specification.getJobId());
                        break;
                    }
                    TimeUnit.SECONDS.sleep(1);
                }
            }
            if (HaApplicationState.terminal(owner) != null) {
                finalizeResources(
                        api, validated(api.getJob(id), uid, encoded, specification.getJobId()));
                return;
            }
            SeaTunnelConfig executionConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            SeatunnelApplicationConfig.configureHa(
                    executionConfig,
                    SeatunnelApplicationConfig.clusterName(id),
                    HaApplicationFactory.masterAddresses(id, parameters),
                    false,
                    specification.getWorkerSpecification().getSlots(),
                    parameters.getHaBackupCount());
            if (claimSubmission(api, owner)) {
                JobConfig job = new JobConfig();
                job.setName(specification.getName());
                try {
                    if (specification.getRestoreJobId() == null) {
                        proxy =
                                client.createExecutionContext(
                                                jobFile.toString(),
                                                null,
                                                job,
                                                executionConfig,
                                                specification.getJobId())
                                        .execute();
                    } else {
                        List<JobPipelineCheckpointData> checkpoints =
                                client.getJobClient()
                                        .getCheckpointData(
                                                specification.getRestoreJobId(),
                                                RestoreMode.CHECKPOINT);
                        if (checkpoints == null || checkpoints.isEmpty()) {
                            log.error(
                                    "No checkpoint found for restoreSourceJobId={}; refusing a fresh start",
                                    specification.getRestoreJobId());
                            recordTerminal(api, owner, ApplicationStatus.FAILED);
                            finalizeResources(
                                    api,
                                    validated(
                                            api.getJob(id),
                                            uid,
                                            encoded,
                                            specification.getJobId()));
                            return;
                        }
                        proxy =
                                client.restoreFromCheckpointExecutionContext(
                                                jobFile.toString(),
                                                null,
                                                job,
                                                executionConfig,
                                                specification.getRestoreJobId(),
                                                specification.getJobId())
                                        .execute();
                    }
                    log.info(
                            "HA application {} submitted native job {}",
                            id,
                            specification.getJobId());
                } catch (Exception ambiguous) {
                    // execute may have reached the server. Preserve intent and observe, never
                    // resubmit.
                    log.warn(
                            "HA application {} submission did not acknowledge; observing fixed job {} ({})",
                            id,
                            specification.getJobId(),
                            ambiguous.getClass().getSimpleName());
                }
            } else {
                log.info(
                        "HA application {} attaching to native job {}",
                        id,
                        specification.getJobId());
            }
            long unresolvedSince = System.nanoTime();
            boolean started = false;
            while (true) {
                owner = validated(api.getJob(id), uid, encoded, specification.getJobId());
                started |=
                        ApplicationStatus.RUNNING
                                .name()
                                .equals(
                                        HaApplicationState.annotations(owner)
                                                .get(HaApplicationState.STATUS));
                if (HaApplicationState.terminal(owner) != null) {
                    break;
                }
                JobStatus status = null;
                try {
                    status = proxy.getJobStatus();
                } catch (Exception transientFailure) {
                    log.debug("Native observation retry for application {}", id);
                }
                ApplicationStatus terminal = terminal(status);
                if (terminal != null) {
                    recordTerminal(api, owner, terminal);
                    break;
                }
                if (!started && status == JobStatus.RUNNING) {
                    recordRunning(api, owner);
                    started = true;
                }
                if (!started
                        && System.nanoTime() - unresolvedSince
                                > TimeUnit.MILLISECONDS.toNanos(
                                        specification.getStartupTimeoutMillis())) {
                    if (recordStartupFailure(api, owner)) {
                        log.error(
                                "HA application {} native startup unresolved within observation budget; refusing resubmission",
                                id);
                        break;
                    }
                    started = true;
                }
                TimeUnit.SECONDS.sleep(1);
            }
        } finally {
            Files.deleteIfExists(jobFile);
        }
        finalizeResources(api, validated(api.getJob(id), uid, encoded, specification.getJobId()));
    }

    /** Only the successful resource-version CAS may send the first native execute request. */
    static boolean claimSubmission(KubernetesClient api, KubernetesJob owner) {
        if (HaApplicationState.terminal(owner) != null
                || !"new"
                        .equals(
                                HaApplicationState.annotations(owner)
                                        .get(HaApplicationState.INTENT))) {
            return false;
        }
        Map<String, String> claimed = new HashMap<>(HaApplicationState.annotations(owner));
        claimed.put(HaApplicationState.INTENT, "requested");
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

    static KubernetesJob validated(KubernetesJob owner, String uid, String encoded, long jobId) {
        if (uid == null
                || !uid.equals(owner.getUid())
                || owner.getInternalResource().getMetadata().getDeletionTimestamp() != null
                || !HaApplicationState.enabled(owner)
                || !Long.toString(jobId)
                        .equals(
                                HaApplicationState.annotations(owner)
                                        .get(HaApplicationState.JOB_ID))
                || !HaApplicationState.digest(encoded)
                        .equals(
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
    static void recordTerminal(
            KubernetesClient api, KubernetesJob owner, ApplicationStatus result) {
        recordStatus(api, owner, result, false);
    }

    static void recordRunning(KubernetesClient api, KubernetesJob owner) {
        recordStatus(api, owner, ApplicationStatus.RUNNING, false);
    }

    /** A startup timeout cannot supersede another observer's durable native RUNNING result. */
    static boolean recordStartupFailure(KubernetesClient api, KubernetesJob owner) {
        return recordStatus(api, owner, ApplicationStatus.FAILED, true);
    }

    private static boolean recordStatus(
            KubernetesClient api,
            KubernetesJob owner,
            ApplicationStatus result,
            boolean startupOnly) {
        for (int attempt = 0; attempt < 10; attempt++) {
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

    static ApplicationStatus terminal(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }
        if (status == JobStatus.CANCELED) {
            return ApplicationStatus.CANCELED;
        }
        return status == JobStatus.FAILED ? ApplicationStatus.FAILED : null;
    }

    static void finalizeResources(KubernetesClient api, KubernetesJob owner) throws Exception {
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
}
