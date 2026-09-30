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

package org.apache.seatunnel.engine.client.cluster.application;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;

import org.apache.seatunnel.engine.client.job.ApplicationJobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;

import java.util.concurrent.TimeUnit;

/** Runs one application job on an existing master, independently of the resource platform. */
public final class ApplicationJobRunner {
    private final SeaTunnelServer server;
    private final ApplicationSpecification specification;

    /** Uses an existing master and its localized, resolved application specification. */
    public ApplicationJobRunner(SeaTunnelServer server, ApplicationSpecification specification) {
        this.server = server;
        this.specification = specification;
    }

    /**
     * Waits for workers, executes the native job and finishes application resources.
     *
     * <p>Resource failure or interruption cancels an unfinished job before resource cleanup. The
     * caller retains ownership of the master and shuts it down after this method returns.
     *
     * @throws Exception if startup, execution, cancellation or resource cleanup fails
     */
    public void run() throws Exception {
        ApplicationResourceManager<?> resources =
                (ApplicationResourceManager<?>) server.getCoordinatorService().getResourceManager();
        CompletableFuture<Void> cancellation = new CompletableFuture<>();
        CompletableFuture<JobResult> execution = null;
        JobResult result = null;
        Exception failure = null;
        try {
            resources.awaitWorkerRegistration();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(specification.getName());
            execution =
                    new ApplicationJobExecutionEnvironment(
                                    jobConfig,
                                    ConfigFactory.parseString(
                                            specification.getJobConfig(),
                                            ConfigParseOptions.defaults()
                                                    .setSyntax(ConfigSyntax.JSON)),
                                    server,
                                    specification.getJobId(),
                                    specification.getOption(ApplicationOptions.RESTORE_JOB_ID))
                            .execute(cancellation);
            result =
                    (JobResult)
                            CompletableFuture.anyOf(execution, resources.getFailureFuture()).get();
        } catch (Exception e) {
            failure = e;
        } finally {
            boolean interrupted = Thread.interrupted() || failure instanceof InterruptedException;
            try {
                if (execution != null && !execution.isDone()) {
                    cancellation.complete(null);
                    try {
                        execution.get(10, TimeUnit.SECONDS);
                    } catch (Exception cleanup) {
                        if (failure == null) {
                            failure = cleanup;
                        } else {
                            failure.addSuppressed(cleanup);
                        }
                    }
                }
                resources.finishApplication(result, failure);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
