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
import java.util.concurrent.TimeUnit;

/**
 * Starts native members or the application's sole native client, without engine lifecycle hooks.
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
            throw new IllegalArgumentException("HA configuration required");
        }
        if ("master".equals(args[1])) {
            startMaster(args[0], parameters);
        } else {
            startApplication(args[0], encoded, parameters);
        }
    }

    private static void startMaster(String id, KubernetesApplicationConfig parameters) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        ApplicationSpecification specification = parameters.getSpecification();
        KubernetesApplicationHaConfig.configure(
                config,
                SeatunnelApplicationConfig.clusterName(id),
                HaApplicationFactory.masterAddresses(id, parameters),
                false,
                specification.getWorkerSpecification().getSlots(),
                parameters.getHaBackupCount());
        SeatunnelApplicationConfig.configureCheckpointRetention(config);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .setPort(specification.getMasterPort())
                .setPortAutoIncrement(false);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        // Native election, resource registration and recovery own this member. It owns no Pods.
        SeaTunnelServerStarter.createHazelcastInstance(config, null, new ResourceManagerFactory());
    }

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
                            api.getJob(id), uid, encoded, specification.getJobId());
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
                Files.deleteIfExists(job);
                KubernetesJob current =
                        HaApplicationState.validated(
                                api.getJob(id), uid, encoded, specification.getJobId());
                if (HaApplicationState.recordStartupFailure(api, current)) {
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

    static List<JobPipelineCheckpointData> restoreCheckpoints(JobClient jobs, Long sourceJobId) {
        if (sourceJobId == null) {
            return Collections.emptyList();
        }
        List<JobPipelineCheckpointData> checkpoints =
                jobs.getCheckpointData(sourceJobId, RestoreMode.CHECKPOINT);
        if (checkpoints == null || checkpoints.isEmpty()) {
            throw new IllegalArgumentException(
                    "No checkpoint found for restore jobId=" + sourceJobId);
        }
        return checkpoints;
    }

    /** Native client operations; attachment and status never submit a replacement job. */
    interface NativeJob {
        boolean ready();

        void prepare() throws Exception;

        void submit() throws Exception;

        JobStatus status();
    }

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
                            api.getJob(id), uid, encoded, specification.getJobId());
            if (HaApplicationState.terminal(owner) != null) {
                break;
            }
            if (HaApplicationState.unsubmitted(owner)) {
                if (workersTerminated(api, id) || System.nanoTime() > deadline) {
                    HaApplicationState.recordStartupFailure(api, owner);
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
                            rejected.getClass().getSimpleName());
                    HaApplicationState.recordStartupFailure(api, owner);
                    continue;
                }
                // Refresh identity after preparation, which can include a remote checkpoint read.
                owner =
                        HaApplicationState.validated(
                                api.getJob(id), uid, encoded, specification.getJobId());
                if (HaApplicationState.claimSubmission(api, owner)) {
                    try {
                        job.submit();
                    } catch (Exception ambiguous) {
                        // The server may have accepted the job. Attach by ID; never execute again.
                        log.warn(
                                "Submission acknowledgement unavailable for application {}: {}",
                                id,
                                ambiguous.getClass().getSimpleName());
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
            ApplicationStatus result = terminalStatus(nativeStatus);
            if (result != null) {
                HaApplicationState.recordTerminal(api, owner, result);
                continue;
            }
            if (workersFailed(api, id, specification.getWorkerCount())) {
                // Fixed worker loss ends this deployment. Deletion, rather than a cancel ACK,
                // ensures no accepted task remains alive after application failure.
                HaApplicationState.recordTerminal(api, owner, ApplicationStatus.FAILED);
                continue;
            }
            if (nativeStatus == JobStatus.RUNNING) {
                HaApplicationState.recordRunning(api, owner);
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
        return workers.size() != expected
                || workers.stream()
                        .anyMatch(p -> p.isTerminated() || p.isTerminating() || p.isUnknown());
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
        while (true) {
            KubernetesJob owner = HaApplicationState.validated(api.getJob(id), uid, encoded, jobId);
            try {
                HaApplicationState.finalizeResources(api, owner);
                return;
            } catch (RuntimeException unavailable) {
                log.warn(
                        "Terminal cleanup will retry for application {}: {}",
                        id,
                        unavailable.getClass().getSimpleName());
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }
}
