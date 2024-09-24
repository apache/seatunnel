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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.rest.servlet.EncryptConfigServlet;
import org.apache.seatunnel.engine.server.rest.servlet.FinishedJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.JobInfoServlet;
import org.apache.seatunnel.engine.server.rest.servlet.OverviewServlet;
import org.apache.seatunnel.engine.server.rest.servlet.RunningJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.StopJobServlet;
import org.apache.seatunnel.engine.server.rest.servlet.StopJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SystemMonitoringServlet;
import org.apache.seatunnel.engine.server.rest.servlet.ThreadDumpServlet;
import org.apache.seatunnel.engine.server.rest.servlet.UpdateTagsServlet;
import org.apache.seatunnel.engine.server.telemetry.metrics.ExportsInstanceInitializer;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.engine.server.rest.RestConstant.ENCRYPT_CONFIG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.FINISHED_JOBS_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.JOB_INFO_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;
import static org.apache.seatunnel.engine.server.rest.RestConstant.THREAD_DUMP;
import static org.apache.seatunnel.engine.server.rest.RestConstant.UPDATE_TAGS_URL;

@Slf4j
public class SeaTunnelServerStarter {

    public static void main(String[] args) {
        createHazelcastInstance();
    }

    public static void createJettyServer(HazelcastInstanceImpl hazelcastInstance) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        Server server = new Server(seaTunnelConfig.getEngineConfig().getHttpConfig().getPort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        context.setResourceBase(
                SeaTunnelServerStarter.class.getClassLoader().getResource("").toExternalForm());
        context.addServlet(
                new org.eclipse.jetty.servlet.ServletHolder(
                        "default", new org.eclipse.jetty.servlet.DefaultServlet()),
                "/");

        ServletHolder overviewHolder = new ServletHolder(new OverviewServlet(hazelcastInstance));
        ServletHolder runningJobsHolder =
                new ServletHolder(new RunningJobsServlet(hazelcastInstance));
        ServletHolder finishedJobsHolder =
                new ServletHolder(new FinishedJobsServlet(hazelcastInstance));
        ServletHolder systemMonitoringHolder =
                new ServletHolder(new SystemMonitoringServlet(hazelcastInstance));
        ServletHolder jobInfoHolder = new ServletHolder(new JobInfoServlet(hazelcastInstance));
        ServletHolder threadDumpHolder =
                new ServletHolder(new ThreadDumpServlet(hazelcastInstance));

        ServletHolder submitJobHolder = new ServletHolder(new SubmitJobServlet(hazelcastInstance));
        ServletHolder submitJobsHolder =
                new ServletHolder(new SubmitJobsServlet(hazelcastInstance));
        ServletHolder stopJobHolder = new ServletHolder(new StopJobServlet(hazelcastInstance));
        ServletHolder stopJobsHolder = new ServletHolder(new StopJobsServlet(hazelcastInstance));
        ServletHolder encryptConfigHolder =
                new ServletHolder(new EncryptConfigServlet(hazelcastInstance));
        ServletHolder updateTagsHandler =
                new ServletHolder(new UpdateTagsServlet(hazelcastInstance));

        context.addServlet(overviewHolder, convertUrlToPath(seaTunnelConfig, OVERVIEW));
        context.addServlet(runningJobsHolder, convertUrlToPath(seaTunnelConfig, RUNNING_JOBS_URL));
        context.addServlet(
                finishedJobsHolder, convertUrlToPath(seaTunnelConfig, FINISHED_JOBS_INFO));
        context.addServlet(
                systemMonitoringHolder,
                convertUrlToPath(seaTunnelConfig, SYSTEM_MONITORING_INFORMATION));
        context.addServlet(jobInfoHolder, convertUrlToPath(seaTunnelConfig, JOB_INFO_URL));
        context.addServlet(threadDumpHolder, convertUrlToPath(seaTunnelConfig, THREAD_DUMP));

        context.addServlet(submitJobHolder, convertUrlToPath(seaTunnelConfig, SUBMIT_JOB_URL));
        context.addServlet(submitJobsHolder, convertUrlToPath(seaTunnelConfig, SUBMIT_JOBS_URL));
        context.addServlet(stopJobHolder, convertUrlToPath(seaTunnelConfig, STOP_JOB_URL));
        context.addServlet(stopJobsHolder, convertUrlToPath(seaTunnelConfig, STOP_JOBS_URL));
        context.addServlet(encryptConfigHolder, convertUrlToPath(seaTunnelConfig, ENCRYPT_CONFIG));
        context.addServlet(updateTagsHandler, convertUrlToPath(seaTunnelConfig, UPDATE_TAGS_URL));

        server.setHandler(context);

        try {
            try {
                server.start();
                server.join();
            } catch (Exception e) {
                log.error("Jetty server start failed", e);
                throw new RuntimeException(e);
            }
        } finally {
            server.destroy();
        }
    }

    public static HazelcastInstanceImpl createHazelcastInstance(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        return createHazelcastInstance(seaTunnelConfig);
    }

    public static HazelcastInstanceImpl createHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        return createHazelcastInstance(seaTunnelConfig, null);
    }

    public static HazelcastInstanceImpl createHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig, String customInstanceName) {
        return initializeHazelcastInstance(seaTunnelConfig, customInstanceName);
    }

    private static HazelcastInstanceImpl initializeHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig, String customInstanceName) {
        boolean condition = checkTelemetryConfig(seaTunnelConfig);
        String instanceName =
                customInstanceName != null
                        ? customInstanceName
                        : HazelcastInstanceFactory.createInstanceName(
                                seaTunnelConfig.getHazelcastConfig());

        HazelcastInstanceImpl original =
                ((HazelcastInstanceProxy)
                                HazelcastInstanceFactory.newHazelcastInstance(
                                        seaTunnelConfig.getHazelcastConfig(),
                                        instanceName,
                                        new SeaTunnelNodeContext(seaTunnelConfig)))
                        .getOriginal();
        // init telemetry instance
        if (condition) {
            initTelemetryInstance(original.node);
        }

        // create jetty server
        createJettyServer(original);

        return original;
    }

    public static HazelcastInstanceImpl createMasterAndWorkerHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig
                .getEngineConfig()
                .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        return ((HazelcastInstanceProxy)
                        HazelcastInstanceFactory.newHazelcastInstance(
                                seaTunnelConfig.getHazelcastConfig(),
                                HazelcastInstanceFactory.createInstanceName(
                                        seaTunnelConfig.getHazelcastConfig()),
                                new SeaTunnelNodeContext(seaTunnelConfig)))
                .getOriginal();
    }

    public static HazelcastInstanceImpl createMasterHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);
        return ((HazelcastInstanceProxy)
                        HazelcastInstanceFactory.newHazelcastInstance(
                                seaTunnelConfig.getHazelcastConfig(),
                                HazelcastInstanceFactory.createInstanceName(
                                        seaTunnelConfig.getHazelcastConfig()),
                                new SeaTunnelNodeContext(seaTunnelConfig)))
                .getOriginal();
    }

    public static HazelcastInstanceImpl createWorkerHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.WORKER);
        // in hazelcast lite node will not store IMap data.
        seaTunnelConfig.getHazelcastConfig().setLiteMember(true);
        return ((HazelcastInstanceProxy)
                        HazelcastInstanceFactory.newHazelcastInstance(
                                seaTunnelConfig.getHazelcastConfig(),
                                HazelcastInstanceFactory.createInstanceName(
                                        seaTunnelConfig.getHazelcastConfig()),
                                new SeaTunnelNodeContext(seaTunnelConfig)))
                .getOriginal();
    }

    public static HazelcastInstanceImpl createHazelcastInstance() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        return createHazelcastInstance(seaTunnelConfig);
    }

    public static void initTelemetryInstance(@NonNull Node node) {
        ExportsInstanceInitializer.init(node);
    }

    private static boolean checkTelemetryConfig(SeaTunnelConfig seaTunnelConfig) {
        // "hazelcast.jmx" need to set "true", for hazelcast metrics
        if (seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().isEnabled()) {
            seaTunnelConfig
                    .getHazelcastConfig()
                    .getProperties()
                    .setProperty("hazelcast.jmx", "true");
            return true;
        }
        return false;
    }

    private static String convertUrlToPath(SeaTunnelConfig seaTunnelConfig, String url) {
        return seaTunnelConfig.getEngineConfig().getHttpConfig().getContextPath() + url + "/*";
    }
}
