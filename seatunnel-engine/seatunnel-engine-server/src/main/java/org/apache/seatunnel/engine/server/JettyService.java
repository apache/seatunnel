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
import org.apache.seatunnel.engine.server.rest.servlet.OverviewServlet;
import org.apache.seatunnel.engine.server.rest.servlet.RunningJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.RunningThreadsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.StopJobServlet;
import org.apache.seatunnel.engine.server.rest.servlet.StopJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SubmitJobsServlet;
import org.apache.seatunnel.engine.server.rest.servlet.SystemMonitoringServlet;
import org.apache.seatunnel.engine.server.rest.servlet.ThreadDumpServlet;
import org.apache.seatunnel.engine.server.rest.servlet.UpdateTagsServlet;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.DispatcherType;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.EnumSet;

import static org.apache.seatunnel.engine.server.rest.RestConstant.ENCRYPT_CONFIG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.FINISHED_JOBS_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_LOG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_LOGS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.JOB_INFO_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_THREADS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.STOP_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SUBMIT_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;
import static org.apache.seatunnel.engine.server.rest.RestConstant.THREAD_DUMP;
import static org.apache.seatunnel.engine.server.rest.RestConstant.UPDATE_TAGS_URL;

/** The Jetty service for SeaTunnel engine server. */
@Slf4j
public class JettyService {
    private static final int MAX_PORT = 65535;

    private NodeEngineImpl nodeEngine;
    private SeaTunnelConfig seaTunnelConfig;
    Server server;

    public JettyService(NodeEngineImpl nodeEngine, SeaTunnelConfig seaTunnelConfig) {
        this.nodeEngine = nodeEngine;
        this.seaTunnelConfig = seaTunnelConfig;
        int port = seaTunnelConfig.getEngineConfig().getHttpConfig().getPort();
        if (seaTunnelConfig.getEngineConfig().getHttpConfig().isEnableDynamicPort()) {
            port = chooseAppropriatePort(port);
        }
        log.info("Jetty will start on port: {}", port);
        this.server = new Server(port);
    }

    public void createJettyServer() {

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(seaTunnelConfig.getEngineConfig().getHttpConfig().getContextPath());

        FilterHolder filterHolder = new FilterHolder(new ExceptionHandlingFilter());
        context.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));

        context.addServlet(new ServletHolder("default", new DefaultServlet()), "/");

        ServletHolder overviewHolder = new ServletHolder(new OverviewServlet(nodeEngine));
        ServletHolder runningJobsHolder = new ServletHolder(new RunningJobsServlet(nodeEngine));
        ServletHolder finishedJobsHolder = new ServletHolder(new FinishedJobsServlet(nodeEngine));
        ServletHolder systemMonitoringHolder =
                new ServletHolder(new SystemMonitoringServlet(nodeEngine));
        ServletHolder jobInfoHolder = new ServletHolder(new JobInfoServlet(nodeEngine));
        ServletHolder threadDumpHolder = new ServletHolder(new ThreadDumpServlet(nodeEngine));

        ServletHolder submitJobHolder = new ServletHolder(new SubmitJobServlet(nodeEngine));
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

        context.addServlet(overviewHolder, convertUrlToPath(OVERVIEW));
        context.addServlet(runningJobsHolder, convertUrlToPath(RUNNING_JOBS_URL));
        context.addServlet(finishedJobsHolder, convertUrlToPath(FINISHED_JOBS_INFO));
        context.addServlet(systemMonitoringHolder, convertUrlToPath(SYSTEM_MONITORING_INFORMATION));
        context.addServlet(jobInfoHolder, convertUrlToPath(JOB_INFO_URL));
        context.addServlet(jobInfoHolder, convertUrlToPath(RUNNING_JOB_URL));
        context.addServlet(threadDumpHolder, convertUrlToPath(THREAD_DUMP));

        context.addServlet(submitJobHolder, convertUrlToPath(SUBMIT_JOB_URL));
        context.addServlet(submitJobsHolder, convertUrlToPath(SUBMIT_JOBS_URL));
        context.addServlet(stopJobHolder, convertUrlToPath(STOP_JOB_URL));
        context.addServlet(stopJobsHolder, convertUrlToPath(STOP_JOBS_URL));
        context.addServlet(encryptConfigHolder, convertUrlToPath(ENCRYPT_CONFIG));
        context.addServlet(updateTagsHandler, convertUrlToPath(UPDATE_TAGS_URL));

        context.addServlet(runningThreadsHolder, convertUrlToPath(RUNNING_THREADS));

        context.addServlet(allNodeLogServletHolder, convertUrlToPath(GET_LOGS));
        context.addServlet(currentNodeLogServlet, convertUrlToPath(GET_LOG));
        context.addServlet(allLogNameServlet, convertUrlToPath(GET_ALL_LOG_NAME));

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

    public int chooseAppropriatePort(int initialPort) {
        int port = initialPort;

        while (port <= MAX_PORT) {
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
