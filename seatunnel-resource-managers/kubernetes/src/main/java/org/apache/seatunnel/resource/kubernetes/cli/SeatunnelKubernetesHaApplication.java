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

import org.apache.seatunnel.api.metadata.MetadataProviderManager;
import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationHaConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesHaClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.HaApplicationFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Member;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Starts native members or the application's sole native client, without engine lifecycle hooks.
 */
@Slf4j
public final class SeatunnelKubernetesHaApplication {
    private SeatunnelKubernetesHaApplication() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 7 && "master".equals(args[1])) {
            startMaster(
                    args[0],
                    args[2],
                    Integer.parseInt(args[3]),
                    Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]),
                    Integer.parseInt(args[6]));
            return;
        }
        // Existing templates may use the full specification even for a master.
        if (args.length != 2 || !("master".equals(args[1]) || "owner".equals(args[1]))) {
            throw new IllegalArgumentException("Expected <application-id> <master|owner>");
        }
        String encoded = System.getenv(KubernetesConstants.SPECIFICATION_ENV);
        KubernetesApplicationConfig parameters = KubernetesApplicationConfig.read(encoded);
        if (!parameters.isHaEnabled()) {
            throw new IllegalArgumentException("HA configuration required");
        }
        if ("master".equals(args[1])) {
            ApplicationSpecification specification = parameters.getSpecification();
            startMaster(
                    args[0],
                    parameters.getNamespace(),
                    parameters.getMasterReplicas(),
                    specification.getMasterPort(),
                    specification.getWorkerSpecification().getSlots(),
                    parameters.getHaBackupCount());
        } else {
            startApplication(args[0], encoded, parameters);
        }
    }

    /** Starts a native HA member; native election/recovery owns it, not Kubernetes Pod cleanup. */
    private static void startMaster(
            String id, String namespace, int replicas, int port, int slots, int backupCount) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        KubernetesApplicationHaConfig.configure(
                config,
                SeatunnelApplicationConfig.clusterName(id),
                HaApplicationFactory.masterAddresses(id, namespace, replicas, port),
                false,
                slots,
                backupCount);
        SeatunnelApplicationConfig.configureCheckpointRetention(config);
        config.getHazelcastConfig().getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        String host = System.getenv(KubernetesConstants.MASTER_HOST_ENV);
        if (host != null && !host.trim().isEmpty()) {
            // A stable discovery name can still resolve to the deleted Pod.
            String formattedHost = host.contains(":") ? "[" + host + "]" : host;
            config.getHazelcastConfig()
                    .getNetworkConfig()
                    .setPublicAddress(formattedHost + ":" + port);
        }
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        // Native election, resource registration and recovery own this member. It owns no Pods.
        SeaTunnelServerStarter.createHazelcastInstance(config, null, new ResourceManagerFactory());
    }

    /**
     * Attaches the sole application client to the validated owner, preparing restore data before
     * claiming submission. Client/temp-file/provider cleanup does not release application compute.
     */
    private static void startApplication(
            String id, String encoded, KubernetesApplicationConfig parameters) throws Exception {
        String uid = System.getenv(HaApplicationFactory.OWNER_UID_ENV);
        ApplicationSpecification specification = parameters.getSpecification();
        try (KubernetesHaClient api =
                KubernetesHaClient.require(
                        KubernetesClientFactory.create(
                                Collections.singletonMap(
                                        KubernetesOptions.NAMESPACE.key(),
                                        parameters.getNamespace()),
                                true))) {
            KubernetesJob owner =
                    HaApplicationState.validated(
                            retryTransient(id, () -> api.getJob(id)),
                            uid,
                            encoded,
                            specification.getJobId());
            if (HaApplicationState.terminal(owner) != null) {
                finish(api, id, uid, encoded, specification.getJobId());
                return;
            }
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(SeatunnelApplicationConfig.clusterName(id));
            clientConfig
                    .getNetworkConfig()
                    .setAddresses(
                            Collections.singletonList(id + ":" + specification.getMasterPort()))
                    .setSmartRouting(false);
            clientConfig
                    .getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(specification.getStartupTimeoutMillis());
            Path job = Files.createTempFile("seatunnel-application-", ".conf");
            SeaTunnelHazelcastClient client;
            try {
                client = new SeaTunnelHazelcastClient(clientConfig);
            } catch (RuntimeException unavailable) {
                log.warn(
                        "Application {} native client connection failed: {}",
                        id,
                        diagnostic(unavailable));
                Files.deleteIfExists(job);
                KubernetesJob current =
                        HaApplicationState.validated(
                                retryTransient(id, () -> api.getJob(id)),
                                uid,
                                encoded,
                                specification.getJobId());
                if (retryTransient(
                        id, () -> HaApplicationState.recordStartupFailure(api, current))) {
                    finish(api, id, uid, encoded, specification.getJobId());
                    return;
                }
                throw unavailable;
            }
            try {
                Files.write(job, specification.getJobConfig().getBytes(StandardCharsets.UTF_8));
                JobClient jobs = new JobClient(client);
                SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
                config.getEngineConfig().getConnectorJarStorageConfig().setEnable(false);
                JobConfig jobConfig = new JobConfig();
                jobConfig.setName(specification.getName());
                RestoreMode mode =
                        specification.getRestoreJobId() == null
                                ? RestoreMode.NONE
                                : RestoreMode.CHECKPOINT;
                // Prepare the native graph before claiming submission. A parser/restore lookup
                // failure is then known not to have accepted work, unlike a lost submit ACK.
                ClientJobExecutionEnvironment environment =
                        new ClientJobExecutionEnvironment(
                                jobConfig,
                                job.toString(),
                                null,
                                client,
                                config,
                                mode,
                                specification.getRestoreJobId(),
                                specification.getJobId()) {
                            private LogicalDag prepared;

                            @Override
                            protected MultipleTableJobConfigParser getJobConfigParser() {
                                // Preserve Application Mode's explicit-restore contract while
                                // loading the native checkpoint payload exactly once for parsing.
                                // Reuse this environment's protected id generator and plugin set
                                // so preparation and execute share one native graph identity.
                                List<JobPipelineCheckpointData> checkpoints =
                                        restoreCheckpoints(jobs, specification.getRestoreJobId());
                                return new MultipleTableJobConfigParser(
                                        job.toString(),
                                        null,
                                        idGenerator,
                                        jobConfig,
                                        commonPluginJars,
                                        mode.isRestore(),
                                        checkpoints,
                                        config.getEngineConfig().getMetadataConfig());
                            }

                            @Override
                            public LogicalDag getLogicalDag() {
                                if (prepared == null) {
                                    prepared = super.getLogicalDag();
                                }
                                return prepared;
                            }
                        };
                runApplication(
                        api,
                        id,
                        uid,
                        encoded,
                        specification,
                        new NativeJob() {
                            @Override
                            public void prepare() {
                                environment.getLogicalDag();
                            }

                            @Override
                            public boolean ready() {
                                long masters =
                                        client.getHazelcastInstance().getCluster().getMembers()
                                                .stream()
                                                .filter(member -> !member.isLiteMember())
                                                .count();
                                long workers =
                                        client.getHazelcastInstance().getCluster().getMembers()
                                                .stream()
                                                .filter(Member::isLiteMember)
                                                .count();
                                return masters == 2 && workers == specification.getWorkerCount();
                            }

                            @Override
                            public void submit() throws Exception {
                                environment.execute();
                            }

                            @Override
                            public JobStatus status() {
                                return jobs.getJobProxy(specification.getJobId()).getJobStatus();
                            }
                        });
            } finally {
                try {
                    client.getHazelcastInstance().shutdown();
                } finally {
                    try {
                        Files.deleteIfExists(job);
                    } finally {
                        MetadataProviderManager.closeProviders();
                    }
                }
            }
        }
    }

    /** Returns no state for a fresh job; an explicitly requested restore must find a checkpoint. */
    static List<JobPipelineCheckpointData> restoreCheckpoints(JobClient jobs, Long sourceJobId) {
        if (sourceJobId == null) {
            return Collections.emptyList();
        }
        List<JobPipelineCheckpointData> checkpoints =
                jobs.getCheckpointData(sourceJobId, RestoreMode.CHECKPOINT);
        if (checkpoints == null || checkpoints.isEmpty()) {
            throw new MissingRestoreCheckpoint(sourceJobId);
        }
        return checkpoints;
    }

    private static final class MissingRestoreCheckpoint extends IllegalArgumentException {
        private MissingRestoreCheckpoint(long sourceJobId) {
            super("No checkpoint found for restore jobId=" + sourceJobId);
        }
    }

    // Job parsing, connectors and API exceptions can include credentials in their messages.
    // Log bounded type/location information, plus messages created here from safe values.
    static String diagnostic(Throwable failure) {
        StringBuilder details = new StringBuilder();
        for (int depth = 0; failure != null && depth < 4; depth++, failure = failure.getCause()) {
            if (depth > 0) {
                details.append(" caused by ");
            }
            details.append(failure.getClass().getName());
            StackTraceElement[] trace = failure.getStackTrace();
            if (trace.length > 0) {
                details.append(" at ").append(trace[0]);
            }
            if (failure instanceof MissingRestoreCheckpoint) {
                details.append(": ").append(failure.getMessage());
            }
        }
        return details.toString();
    }

    private static <T> T retryTransient(String id, Supplier<T> operation)
            throws InterruptedException {
        long delay = 1;
        while (true) {
            try {
                return operation.get();
            } catch (KubernetesClientException failure) {
                if (!failure.isTransientFailure()) {
                    throw failure;
                }
                log.warn(
                        "Application {} Kubernetes request failed (HTTP {}), retrying in {}s",
                        id,
                        failure.getCode(),
                        delay);
                TimeUnit.SECONDS.sleep(delay);
                delay = Math.min(30, delay * 2);
            }
        }
    }

    private static void retryTransient(String id, Runnable operation) throws InterruptedException {
        retryTransient(
                id,
                () -> {
                    operation.run();
                    return null;
                });
    }

    /** Native client operations; attachment and status never submit a replacement job. */
    interface NativeJob {
        boolean ready();

        void prepare() throws Exception;

        void submit() throws Exception;

        JobStatus status();
    }

    /**
     * Claims native submission once, then observes the same job ID even after an ambiguous submit
     * acknowledgement. Only a recorded terminal outcome permits finalization; unavailable status
     * never authorizes a replacement execution. Unhandled API failures remain errors, not success.
     */
    static void runApplication(
            KubernetesHaClient api,
            String id,
            String uid,
            String encoded,
            ApplicationSpecification specification,
            NativeJob job)
            throws Exception {
        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(specification.getStartupTimeoutMillis());
        while (true) {
            KubernetesJob owner =
                    HaApplicationState.validated(
                            retryTransient(id, () -> api.getJob(id)),
                            uid,
                            encoded,
                            specification.getJobId());
            if (HaApplicationState.terminal(owner) != null) {
                break;
            }
            KubernetesJob statusOwner = owner;
            if (HaApplicationState.unsubmitted(owner)) {
                if (retryTransient(id, () -> workersTerminated(api, id))
                        || System.nanoTime() > deadline) {
                    retryTransient(id, () -> HaApplicationState.recordStartupFailure(api, owner));
                    continue;
                }
                if (!job.ready()) {
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                }
                try {
                    job.prepare();
                } catch (Exception rejected) {
                    log.warn(
                            "Application {} graph preparation failed: {}",
                            id,
                            diagnostic(rejected));
                    retryTransient(id, () -> HaApplicationState.recordStartupFailure(api, owner));
                    continue;
                }
                // Refresh identity after preparation, which can include a remote checkpoint read.
                KubernetesJob refreshed =
                        HaApplicationState.validated(
                                retryTransient(id, () -> api.getJob(id)),
                                uid,
                                encoded,
                                specification.getJobId());
                statusOwner = refreshed;
                String claimId = UUID.randomUUID().toString();
                boolean claimed =
                        retryTransient(
                                id,
                                () -> HaApplicationState.claimSubmission(api, refreshed, claimId));
                if (!claimed) {
                    // The previous CAS may have committed before its response was lost.
                    // Only this process's unique claim can authorize a native submission.
                    statusOwner =
                            HaApplicationState.validated(
                                    retryTransient(id, () -> api.getJob(id)),
                                    uid,
                                    encoded,
                                    specification.getJobId());
                    claimed = HaApplicationState.claimedBy(statusOwner, claimId);
                }
                if (claimed) {
                    try {
                        job.submit();
                    } catch (Exception ambiguous) {
                        // The server may have accepted the job. Attach by ID; never execute again.
                        log.warn(
                                "Submission acknowledgement unavailable for application {}: {}",
                                id,
                                diagnostic(ambiguous));
                    }
                }
            }
            JobStatus nativeStatus = null;
            try {
                nativeStatus = job.status();
            } catch (RuntimeException unavailable) {
                log.debug(
                        "Native status temporarily unavailable for application {}: {}",
                        id,
                        unavailable.getClass().getSimpleName());
            }
            KubernetesJob observedOwner = statusOwner;
            ApplicationStatus result = terminalStatus(nativeStatus);
            if (result != null) {
                retryTransient(
                        id, () -> HaApplicationState.recordTerminal(api, observedOwner, result));
                continue;
            }
            if (retryTransient(id, () -> workersFailed(api, id, specification.getWorkerCount()))) {
                // Confirmed fixed worker loss ends the deployment. Kubernetes deletion
                // cannot fence an accepted writer if a node is unreachable.
                retryTransient(
                        id,
                        () ->
                                HaApplicationState.recordTerminal(
                                        api, owner, ApplicationStatus.FAILED));
                continue;
            }
            if (nativeStatus == JobStatus.RUNNING) {
                retryTransient(id, () -> HaApplicationState.recordRunning(api, observedOwner));
            }
            // UNKNOWABLE is not a terminal result. A lost admission/restore requires explicit
            // cancellation if the native engine cannot recover it. It never permits resubmission.
            TimeUnit.SECONDS.sleep(1);
        }
        finish(api, id, uid, encoded, specification.getJobId());
    }

    private static boolean workersTerminated(KubernetesHaClient api, String id) {
        return api
                .listPods(KubernetesResourceIdentity.selector(id, KubernetesConstants.WORKER_ROLE))
                .stream()
                .anyMatch(p -> p.isTerminated() || p.isTerminating() || p.isUnknown());
    }

    private static boolean workersFailed(KubernetesHaClient api, String id, int expected) {
        List<KubernetesPod> workers =
                api.listPods(
                        KubernetesResourceIdentity.selector(id, KubernetesConstants.WORKER_ROLE));
        // Unknown and terminating Pods can still be running. Keep observing the
        // same native job until it reaches a terminal state or worker loss is observed.
        return workers.size() != expected || workers.stream().anyMatch(KubernetesPod::isTerminated);
    }

    static ApplicationStatus terminalStatus(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }
        if (status == JobStatus.FAILED) {
            return ApplicationStatus.FAILED;
        }
        return status == JobStatus.CANCELED ? ApplicationStatus.CANCELED : null;
    }

    private static void finish(
            KubernetesHaClient api, String id, String uid, String encoded, long jobId)
            throws Exception {
        finish(api, id, uid, encoded, jobId, TimeUnit.SECONDS::sleep);
    }

    @FunctionalInterface
    interface RetryWait {
        void sleep(long seconds) throws InterruptedException;
    }

    static void finish(
            KubernetesHaClient api,
            String id,
            String uid,
            String encoded,
            long jobId,
            RetryWait wait)
            throws Exception {
        long retrySeconds = 1;
        while (true) {
            KubernetesJob owner;
            try {
                owner = api.getJob(id);
            } catch (KubernetesClientException unavailable) {
                // An absent owner cannot authorize cleanup; other API failures may recover.
                if (unavailable.getCode() == 404) {
                    throw unavailable;
                }
                retrySeconds = awaitCleanupRetry(id, retrySeconds, unavailable, wait);
                continue;
            }
            // Identity changes and nonterminal state are safety failures, not retryable outages.
            HaApplicationState.validated(owner, uid, encoded, jobId);
            if (HaApplicationState.terminal(owner) == null) {
                throw new IllegalStateException("Nonterminal application cannot release compute");
            }
            try {
                HaApplicationState.finalizeResources(api, owner);
                return;
            } catch (RuntimeException unavailable) {
                retrySeconds = awaitCleanupRetry(id, retrySeconds, unavailable, wait);
            }
        }
    }

    private static long awaitCleanupRetry(
            String id, long seconds, RuntimeException failure, RetryWait wait)
            throws InterruptedException {
        log.warn(
                "Terminal cleanup will retry in {}s for application {}: {}",
                seconds,
                id,
                failure.getClass().getSimpleName());
        wait.sleep(seconds);
        // Bound API/log traffic during an outage without turning unknown native work terminal.
        return Math.min(30, seconds * 2);
    }
}
