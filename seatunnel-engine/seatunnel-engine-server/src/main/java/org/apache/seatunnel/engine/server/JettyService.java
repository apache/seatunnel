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

import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;
import org.apache.seatunnel.shade.org.eclipse.jetty.servlet.DefaultServlet;
import org.apache.seatunnel.shade.org.eclipse.jetty.servlet.FilterHolder;
import org.apache.seatunnel.shade.org.eclipse.jetty.servlet.ServletContextHandler;
import org.apache.seatunnel.shade.org.eclipse.jetty.servlet.ServletHolder;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.rest.filter.ExceptionHandlingFilter;
import org.apache.seatunnel.engine.server.rest.servlet.AllLogNameServlet;
import org.apache.seatunnel.engine.server.rest.servlet.AllNodeLogServlet;
import org.apache.seatunnel.engine.server.rest.servlet.CurrentNodeLogServlet;
import org.apache.seatunnel.engine.server.rest.servlet.EncryptConfigServlet;
import org.apache.seatunnel.engine.server.rest.servlet.FinishedJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.JobInfoServlet;
import org.apache.seatunnel.engine.server.rest.servlet.MetricsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.OverviewServlet;
import org.apache.seatunnel.engine.server.rest.servlet.RunningJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.RunningThreadsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.StopJobServlet;
import org.apache.seatunnel.engine.server.rest.servlet.StopJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobByUploadFileServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SystemMonitoringServlet;
import org.apache.seatunnel.engine.server.rest.servlet.ThreadDumpServlet;
import org.apache.seatunnel.engine.server.rest.servlet.UpdateTagsServlet;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.DispatcherType;
import javax.servlet.MultipartConfigElement;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URL;
import java.util.EnumSet;

import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_ENCRYPT_CONFIG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_FINISHED_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_JOB_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOGS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_METRICS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_OPEN_METRICS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_RUNNING_JOB;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_RUNNING_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_RUNNING_THREADS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_STOP_JOB;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_STOP_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SUBMIT_JOB;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SUBMIT_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SUBMIT_JOB_BY_UPLOAD_FILE;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SYSTEM_MONITORING_INFORMATION;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_THREAD_DUMP;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_UPDATE_TAGS;

/** The Jetty service for SeaTunnel engine server. */
@Slf4j
public class JettyService {

    private NodeEngineImpl nodeEngine;
    private SeaTunnelConfig seaTunnelConfig;
    Server server;

    public JettyService(NodeEngineImpl nodeEngine, SeaTunnelConfig seaTunnelConfig) {
        this.nodeEngine = nodeEngine;
        this.seaTunnelConfig = seaTunnelConfig;
        int port = seaTunnelConfig.getEngineConfig().getHttpConfig().getPort();
        if (seaTunnelConfig.getEngineConfig().getHttpConfig().isEnableDynamicPort()) {
            port =
                    chooseAppropriatePort(
                            port, seaTunnelConfig.getEngineConfig().getHttpConfig().getPortRange());
        }
        log.info("SeaTunnel REST service will start on port {}", port);
        this.server = new Server(port);
    }

    public void createJettyServer() {

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(seaTunnelConfig.getEngineConfig().getHttpConfig().getContextPath());

        FilterHolder filterHolder = new FilterHolder(new ExceptionHandlingFilter());
        context.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));

        ServletHolder defaultServlet = new ServletHolder("default", DefaultServlet.class);
        URL uiResource = JettyService.class.getClassLoader().getResource("ui");
        if (uiResource != null) {
            defaultServlet.setInitParameter("resourceBase", uiResource.toExternalForm());
        } else {
            log.warn("UI resources not found in classpath");
        }

        context.addServlet(defaultServlet, "/");

        ServletHolder overviewHolder = new ServletHolder(new OverviewServlet(nodeEngine));
        ServletHolder runningJobsHolder = new ServletHolder(new RunningJobsServlet(nodeEngine));
        ServletHolder finishedJobsHolder = new ServletHolder(new FinishedJobsServlet(nodeEngine));
        ServletHolder systemMonitoringHolder =
                new ServletHolder(new SystemMonitoringServlet(nodeEngine));
        ServletHolder jobInfoHolder = new ServletHolder(new JobInfoServlet(nodeEngine));
        ServletHolder threadDumpHolder = new ServletHolder(new ThreadDumpServlet(nodeEngine));

        ServletHolder submitJobHolder = new ServletHolder(new SubmitJobServlet(nodeEngine));
        ServletHolder submitJobByUploadFileHolder =
                new ServletHolder(new SubmitJobByUploadFileServlet(nodeEngine));

        ServletHolder submitJobsHolder = new ServletHolder(new SubmitJobsServlet(nodeEngine));
        ServletHolder stopJobHolder = new ServletHolder(new StopJobServlet(nodeEngine));
        ServletHolder stopJobsHolder = new ServletHolder(new StopJobsServlet(nodeEngine));
        ServletHolder encryptConfigHolder = new ServletHolder(new EncryptConfigServlet(nodeEngine));
        ServletHolder updateTagsHandler = new ServletHolder(new UpdateTagsServlet(nodeEngine));

        ServletHolder runningThreadsHolder =
                new ServletHolder(new RunningThreadsServlet(nodeEngine));

        ServletHolder allNodeLogServletHolder =
                new ServletHolder(new AllNodeLogServlet(nodeEngine));
        ServletHolder currentNodeLogServlet =
                new ServletHolder(new CurrentNodeLogServlet(nodeEngine));
        ServletHolder allLogNameServlet = new ServletHolder(new AllLogNameServlet(nodeEngine));

        ServletHolder metricsServlet = new ServletHolder(new MetricsServlet(nodeEngine));

        context.addServlet(overviewHolder, convertUrlToPath(REST_URL_OVERVIEW));
        context.addServlet(runningJobsHolder, convertUrlToPath(REST_URL_RUNNING_JOBS));
        context.addServlet(finishedJobsHolder, convertUrlToPath(REST_URL_FINISHED_JOBS));
        context.addServlet(
                systemMonitoringHolder, convertUrlToPath(REST_URL_SYSTEM_MONITORING_INFORMATION));
        context.addServlet(jobInfoHolder, convertUrlToPath(REST_URL_JOB_INFO));
        context.addServlet(jobInfoHolder, convertUrlToPath(REST_URL_RUNNING_JOB));
        context.addServlet(threadDumpHolder, convertUrlToPath(REST_URL_THREAD_DUMP));
        MultipartConfigElement multipartConfigElement = new MultipartConfigElement("");
        submitJobByUploadFileHolder.getRegistration().setMultipartConfig(multipartConfigElement);
        context.addServlet(
                submitJobByUploadFileHolder, convertUrlToPath(REST_URL_SUBMIT_JOB_BY_UPLOAD_FILE));
        context.addServlet(submitJobHolder, convertUrlToPath(REST_URL_SUBMIT_JOB));
        context.addServlet(submitJobsHolder, convertUrlToPath(REST_URL_SUBMIT_JOBS));
        context.addServlet(stopJobHolder, convertUrlToPath(REST_URL_STOP_JOB));
        context.addServlet(stopJobsHolder, convertUrlToPath(REST_URL_STOP_JOBS));
        context.addServlet(encryptConfigHolder, convertUrlToPath(REST_URL_ENCRYPT_CONFIG));
        context.addServlet(updateTagsHandler, convertUrlToPath(REST_URL_UPDATE_TAGS));

        context.addServlet(runningThreadsHolder, convertUrlToPath(REST_URL_RUNNING_THREADS));

        context.addServlet(allNodeLogServletHolder, convertUrlToPath(REST_URL_LOGS));
        context.addServlet(currentNodeLogServlet, convertUrlToPath(REST_URL_LOG));
        context.addServlet(allLogNameServlet, convertUrlToPath(REST_URL_GET_ALL_LOG_NAME));
        context.addServlet(metricsServlet, convertUrlToPath(REST_URL_METRICS));
        context.addServlet(metricsServlet, convertUrlToPath(REST_URL_OPEN_METRICS));

        server.setHandler(context);

        try {
            server.start();
        } catch (Exception e) {
            log.error("Jetty server start failed", e);
            throw new RuntimeException(e);
        }
    }

    public void shutdownJettyServer() {
        try {
            server.stop();
        } catch (Exception e) {
            log.error("Jetty server stop failed", e);
            throw new RuntimeException(e);
        }
    }

    private static String convertUrlToPath(String url) {
        return url + "/*";
    }

    public int chooseAppropriatePort(int initialPort, int portRange) {
        int port = initialPort;

        while (port <= initialPort + portRange) {
            if (!isPortInUse(port)) {
                return port;
            }
            port++;
        }

        throw new RuntimeException("Jetty failed to start, No available port found in the range!");
    }

    private boolean isPortInUse(int port) {
        try (ServerSocket ss = new ServerSocket(port);
                DatagramSocket ds = new DatagramSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }
}
