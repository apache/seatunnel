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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.NodeExtension;
import org.apache.seatunnel.engine.server.log.FormatType;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.rest.service.JobInfoService;
import org.apache.seatunnel.engine.server.rest.service.LogService;
import org.apache.seatunnel.engine.server.rest.service.OptionRulesService;
import org.apache.seatunnel.engine.server.rest.service.OverviewService;
import org.apache.seatunnel.engine.server.rest.service.RunningThreadService;
import org.apache.seatunnel.engine.server.rest.service.SystemMonitoringService;
import org.apache.seatunnel.engine.server.rest.service.ThreadDumpService;
import org.apache.seatunnel.engine.server.rest.service.TraceTaskMappingService;

import com.google.gson.Gson;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_404;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.INSTANCE_CONTEXT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_FINISHED_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_JOB_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOGS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_METRICS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_OPEN_METRICS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_OPTION_RULES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_RUNNING_JOB;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_RUNNING_JOBS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_RUNNING_THREADS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SYSTEM_MONITORING_INFORMATION;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_THREAD_DUMP;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_TRACE_TASK_MAPPING;

/**
 * Dispatches Hazelcast ASCII GET requests to SeaTunnel-specific overview, log, and trace services.
 */
@Slf4j
public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final Log4j2HttpGetCommandProcessor original;
    private NodeEngineImpl nodeEngine;
    private OverviewService overviewService;
    private JobInfoService jobInfoService;
    private SystemMonitoringService systemMonitoringService;
    private ThreadDumpService threadDumpService;
    private RunningThreadService runningThreadService;
    private LogService logService;
    private TraceTaskMappingService traceTaskMappingService;
    private OptionRulesService optionRulesService;

    public RestHttpGetCommandProcessor(TextCommandService textCommandService) {

        this(textCommandService, new Log4j2HttpGetCommandProcessor(textCommandService));
        this.nodeEngine = this.textCommandService.getNode().getNodeEngine();
        this.overviewService = new OverviewService(nodeEngine);
        this.jobInfoService = new JobInfoService(nodeEngine);
        this.systemMonitoringService = new SystemMonitoringService(nodeEngine);
        this.threadDumpService = new ThreadDumpService(nodeEngine);
        this.runningThreadService = new RunningThreadService(nodeEngine);
        this.logService = new LogService(nodeEngine);
        this.traceTaskMappingService = new TraceTaskMappingService(nodeEngine);
        this.optionRulesService = new OptionRulesService(nodeEngine);
    }

    public RestHttpGetCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpGetCommandProcessor log4j2HttpGetCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = log4j2HttpGetCommandProcessor;
        this.nodeEngine = this.textCommandService.getNode().getNodeEngine();
        this.overviewService = new OverviewService(nodeEngine);
        this.jobInfoService = new JobInfoService(nodeEngine);
        this.systemMonitoringService = new SystemMonitoringService(nodeEngine);
        this.threadDumpService = new ThreadDumpService(nodeEngine);
        this.runningThreadService = new RunningThreadService(nodeEngine);
        this.logService = new LogService(nodeEngine);
        this.traceTaskMappingService = new TraceTaskMappingService(nodeEngine);
        this.optionRulesService = new OptionRulesService(nodeEngine);
    }

    /**
     * Routes each GET request to the matching SeaTunnel REST service or falls back to Hazelcast.
     */
    @Override
    public void handle(HttpGetCommand httpGetCommand) {
        String uri = httpGetCommand.getURI();

        try {
            if (uri.startsWith(CONTEXT_PATH + REST_URL_RUNNING_JOBS)) {
                handleRunningJobsInfo(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_FINISHED_JOBS)) {
                handleFinishedJobsInfo(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_RUNNING_JOB)
                    || uri.startsWith(CONTEXT_PATH + REST_URL_JOB_INFO)) {
                handleJobInfoById(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_SYSTEM_MONITORING_INFORMATION)) {
                getSystemMonitoringInformation(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_RUNNING_THREADS)) {
                getRunningThread(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_OVERVIEW)) {
                overView(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_OPTION_RULES)) {
                getOptionRules(httpGetCommand, uri);
            } else if (uri.equals(INSTANCE_CONTEXT_PATH + REST_URL_METRICS)) {
                handleMetrics(httpGetCommand, TextFormat.CONTENT_TYPE_004);
            } else if (uri.equals(INSTANCE_CONTEXT_PATH + REST_URL_OPEN_METRICS)) {
                handleMetrics(httpGetCommand, TextFormat.CONTENT_TYPE_OPENMETRICS_100);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_THREAD_DUMP)) {
                getThreadDump(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_GET_ALL_LOG_NAME)) {
                getAllLogName(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_LOGS)) {
                getAllNodeLog(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_LOG)) {
                getCurrentNodeLog(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + REST_URL_TRACE_TASK_MAPPING)) {
                handleTraceTaskMapping(httpGetCommand, uri);
            } else {
                original.handle(httpGetCommand);
            }
        } catch (IndexOutOfBoundsException e) {
            httpGetCommand.send400();
        } catch (IllegalArgumentException e) {
            prepareResponse(SC_400, httpGetCommand, exceptionResponse(e));
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + httpGetCommand, e);
            prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
        }

        this.textCommandService.sendResponse(httpGetCommand);
    }

    @Override
    public void handleRejection(HttpGetCommand httpGetCommand) {
        handle(httpGetCommand);
    }

    public void overView(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        String tagStr;
        if (uri.contains("?")) {
            int index = uri.indexOf("?");
            tagStr = uri.substring(index + 1);
        } else {
            tagStr = "";
        }
        Map<String, String> tags =
                Arrays.stream(tagStr.split("&"))
                        .map(variable -> variable.split("=", 2))
                        .filter(pair -> pair.length == 2)
                        .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));

        this.prepareResponse(
                command,
                JsonUtil.toJsonObject(
                        JsonUtils.toMap(
                                JsonUtils.toJsonString(overviewService.getOverviewInfo(tags)))));
    }

    private void getOptionRules(HttpGetCommand command, String uri) {
        try {
            Map<String, String> params = getUriParam(uri);
            String response =
                    new Gson()
                            .toJson(
                                    optionRulesService.getOptionRules(
                                            params.get("type"), params.get("plugin")));
            this.prepareResponse(command, Json.parse(response).asObject());
        } catch (java.util.NoSuchElementException e) {
            prepareResponse(SC_404, command, exceptionResponse(e));
        }
    }

    public void getThreadDump(HttpGetCommand command) {

        this.prepareResponse(command, threadDumpService.getThreadDump());
    }

    private void getSystemMonitoringInformation(HttpGetCommand command) {
        this.prepareResponse(
                command, systemMonitoringService.getSystemMonitoringInformationJsonValues());
    }

    private void handleRunningJobsInfo(HttpGetCommand command) {
        this.prepareResponse(command, jobInfoService.getRunningJobsJson());
    }

    private void handleFinishedJobsInfo(HttpGetCommand command, String uri) {

        uri = StringUtil.stripTrailingSlash(uri);

        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String state;
        if (indexEnd == -1) {
            state = "";
        } else {
            state = uri.substring(indexEnd + 1);
        }

        this.prepareResponse(command, jobInfoService.getJobsByStateJson(state));
    }

    private void handleJobInfoById(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String jobId = uri.substring(indexEnd + 1);
        this.prepareResponse(command, jobInfoService.getJobInfoJson(Long.valueOf(jobId)));
    }

    private void getRunningThread(HttpGetCommand command) {
        this.prepareResponse(command, runningThreadService.getRunningThread());
    }

    /**
     * Parses the job id from the trace mapping endpoint and returns the current task mapping JSON.
     */
    private void handleTraceTaskMapping(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        String prefix = CONTEXT_PATH + REST_URL_TRACE_TASK_MAPPING + "/";
        if (!uri.startsWith(prefix)) {
            command.send400();
            return;
        }
        String jobIdStr = uri.substring(prefix.length());
        long jobId = Long.parseLong(jobIdStr);
        this.prepareResponse(command, traceTaskMappingService.getJobTaskMappingJson(jobId));
    }

    private void handleMetrics(HttpGetCommand httpGetCommand, String contentType) {
        log.info("Metrics request received");
        StringWriter stringWriter = new StringWriter();
        NodeExtension nodeExtension =
                (NodeExtension) textCommandService.getNode().getNodeExtension();
        try {
            TextFormat.writeFormat(
                    contentType,
                    stringWriter,
                    nodeExtension.getCollectorRegistry().metricFamilySamples());
            this.prepareResponse(httpGetCommand, stringWriter.toString());
        } catch (IOException e) {
            httpGetCommand.send400();
        } finally {
            try {
                stringWriter.close();
            } catch (IOException e) {
                logger.warning("An error occurred while handling request " + httpGetCommand, e);
                prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
            }
        }
    }

    private void getAllNodeLog(HttpGetCommand httpGetCommand, String uri) {

        // Analysis uri, get logName and jobId param
        String param = getParam(uri);
        boolean isLogFile = param.contains(".log");
        String logName = isLogFile ? param : StringUtils.EMPTY;
        String jobId = !isLogFile ? param : StringUtils.EMPTY;

        String logPath = logService.getLogPath();
        if (StringUtils.isBlank(logPath)) {
            logger.warning(
                    "Log file path is empty, no log file path configured in the current configuration file");
            httpGetCommand.send404();
            return;
        }

        if (StringUtils.isBlank(logName)) {
            FormatType formatType = getFormatType(uri);
            switch (formatType) {
                case JSON:
                    this.prepareResponse(httpGetCommand, logService.allNodeLogFormatJson(jobId));
                    return;
                case HTML:
                default:
                    this.prepareResponse(
                            httpGetCommand, getRestValue(logService.allNodeLogFormatHtml(jobId)));
            }
        } else {
            prepareLogResponse(httpGetCommand, logPath, logName);
        }
    }

    private FormatType getFormatType(String uri) {
        Map<String, String> uriParam = getUriParam(uri);
        return FormatType.fromString(uriParam.get("format"));
    }

    private Map<String, String> getUriParam(String uri) {
        String queryString = uri.contains("?") ? uri.substring(uri.indexOf("?") + 1) : "";
        return Arrays.stream(queryString.split("&"))
                .map(param -> param.split("=", 2))
                .filter(pair -> pair.length == 2)
                .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));
    }

    private String getParam(String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        if (indexEnd != -1) {
            String param = uri.substring(indexEnd + 1);
            logger.fine(String.format("Request: %s , Param: %s", uri, param));
            return param;
        }
        return StringUtils.EMPTY;
    }

    private static RestValue getRestValue(String logContent) {
        RestValue restValue = new RestValue();
        restValue.setContentType("text/html; charset=UTF-8".getBytes(StandardCharsets.UTF_8));
        restValue.setValue(logContent.getBytes(StandardCharsets.UTF_8));
        return restValue;
    }

    /** Get Current Node Log By /log request */
    private void getCurrentNodeLog(HttpGetCommand httpGetCommand, String uri) {
        String logName = getParam(uri);
        String logPath = logService.getLogPath();

        if (StringUtils.isBlank(logName)) {
            // Get Current Node Log List
            this.prepareResponse(httpGetCommand, getRestValue(logService.currentNodeLog()));
        } else {
            // Get Current Node Log Content
            prepareLogResponse(httpGetCommand, logPath, logName);
        }
    }

    /**
     * Prepares the current-node log response after enforcing the configured log directory boundary.
     *
     * <p>The requested log file is resolved to its canonical path before reading so that relative
     * segments and symbolic links cannot escape the canonical log directory.
     *
     * @param httpGetCommand command used to send the HTTP response
     * @param logPath configured log directory
     * @param logName requested log file name from the request URI
     */
    private void prepareLogResponse(HttpGetCommand httpGetCommand, String logPath, String logName) {
        String logFilePath = new File(logPath, logName).getPath();
        try {
            String canonicalLogDir = new File(logPath).getCanonicalPath();
            String canonicalFilePath = new File(logFilePath).getCanonicalPath();
            if (!canonicalFilePath.startsWith(canonicalLogDir + File.separator)
                    && !canonicalFilePath.equals(canonicalLogDir)) {
                httpGetCommand.send400();
                logger.warning(
                        String.format(
                                "Path traversal attempt blocked - Requested: %s, Resolved: %s, LogDir: %s",
                                logName, canonicalFilePath, canonicalLogDir));
                return;
            }
            String logContent = FileUtils.readFileToStr(new File(canonicalFilePath).toPath());
            this.prepareResponse(httpGetCommand, logContent);
        } catch (IOException e) {
            httpGetCommand.send400();
            logger.warning(
                    String.format(
                            "Failed to resolve log file path: %s, error: %s",
                            logFilePath, e.getMessage()));
        } catch (SeaTunnelRuntimeException e) {
            httpGetCommand.send400();
            logger.warning(
                    String.format("Log file content is empty, get log path : %s", logFilePath));
        }
    }

    private void getAllLogName(HttpGetCommand httpGetCommand) {

        try {
            this.prepareResponse(httpGetCommand, JsonUtils.toJsonString(logService.allLogName()));
        } catch (SeaTunnelRuntimeException e) {
            httpGetCommand.send400();
            logger.warning(
                    String.format(
                            "Log file name get failed, get log path: %s", logService.getLogPath()));
        }
    }
}
