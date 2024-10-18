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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.enums.DiscoveryType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.core.starter.flink.FlinkStarter;
import org.apache.seatunnel.core.starter.flink.utils.ResourceUtils;
import org.apache.seatunnel.translation.flink.metric.FlinkJobMetricsSummary;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.core.starter.flink.execution.FlinkAbstractPluginExecuteProcessor.ADD_URL_TO_CLASSLOADER;

/** Used to execute a SeaTunnelTask. */
public class FlinkExecution implements TaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);

    private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
    private final PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment>
            sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment>
            transformPluginExecuteProcessor;
    private final PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment>
            sinkPluginExecuteProcessor;
    private List<URL> actualUsedJars;
    private final List<URL> connectors = new ArrayList<>();

    public FlinkExecution(DiscoveryType type, List<URL> connectors, Config config) {
        initializeClasspath(type, connectors, config);
        Config envConfig = config.getConfig("env");
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));
        this.sourcePluginExecuteProcessor =
                new SourceExecuteProcessor(
                        type,
                        this.connectors,
                        actualUsedJars,
                        envConfig,
                        config.getConfigList(Constants.SOURCE),
                        jobContext);
        this.transformPluginExecuteProcessor =
                new TransformExecuteProcessor(
                        type,
                        this.connectors,
                        actualUsedJars,
                        envConfig,
                        TypesafeConfigUtils.getConfigList(
                                config, Constants.TRANSFORM, Collections.emptyList()),
                        jobContext);
        this.sinkPluginExecuteProcessor =
                new SinkExecuteProcessor(
                        type,
                        this.connectors,
                        actualUsedJars,
                        envConfig,
                        config.getConfigList(Constants.SINK),
                        jobContext);

        LOGGER.info(
                "SeaTunnel task actual plugin classpath: {}",
                JsonUtils.toJsonString(actualUsedJars));
        this.flinkRuntimeEnvironment =
                FlinkRuntimeEnvironment.getInstance(
                        this.registerPlugin(type, config, actualUsedJars));

        this.sourcePluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.transformPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.sinkPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
    }

    @Override
    public void execute() throws TaskExecuteException {
        List<DataStreamTableInfo> dataStreams = new ArrayList<>();
        dataStreams = sourcePluginExecuteProcessor.execute(dataStreams);
        dataStreams = transformPluginExecuteProcessor.execute(dataStreams);
        sinkPluginExecuteProcessor.execute(dataStreams);
        LOGGER.info(
                "Flink Execution Plan: {}",
                flinkRuntimeEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
        LOGGER.info("Flink job name: {}", flinkRuntimeEnvironment.getJobName());
        if (!flinkRuntimeEnvironment.isStreaming()) {
            flinkRuntimeEnvironment
                    .getStreamExecutionEnvironment()
                    .setRuntimeMode(RuntimeExecutionMode.BATCH);
            LOGGER.info("Flink job Mode: {}", JobMode.BATCH);
        }
        try {
            final long jobStartTime = System.currentTimeMillis();
            JobExecutionResult jobResult =
                    flinkRuntimeEnvironment
                            .getStreamExecutionEnvironment()
                            .execute(flinkRuntimeEnvironment.getJobName());
            final long jobEndTime = System.currentTimeMillis();

            final FlinkJobMetricsSummary jobMetricsSummary =
                    FlinkJobMetricsSummary.builder()
                            .jobExecutionResult(jobResult)
                            .jobStartTime(jobStartTime)
                            .jobEndTime(jobEndTime)
                            .build();

            LOGGER.info("Job finished, execution result: \n{}", jobMetricsSummary);
        } catch (Exception e) {
            throw new TaskExecuteException("Execute Flink job error", e);
        }
    }

    private void initializeClasspath(DiscoveryType type, List<URL> connectors, Config config) {
        switch (type) {
            case LOCAL:
                initializeLocalClasspath(config);
                break;
            case REMOTE:
                initializeRemoteClasspath(config, connectors);
                break;
            default:
                throw new IllegalArgumentException("unsupported plugin discover type: " + type);
        }
    }

    private void initializeLocalClasspath(Config config) {
        try {
            actualUsedJars =
                    new ArrayList<>(
                            Collections.singletonList(
                                    new File(
                                                    Common.appStarterDir()
                                                            .resolve(FlinkStarter.APP_JAR_NAME)
                                                            .toString())
                                            .toURI()
                                            .toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        Config envConfig = config.getConfig("env");
        registerPlugin(DiscoveryType.LOCAL, envConfig);
    }

    private void initializeRemoteClasspath(Config config, List<URL> connectors) {
        if (connectors == null || connectors.isEmpty()) {
            throw new SeaTunnelException("provide remote connectors empty.");
        }
        this.actualUsedJars = new ArrayList<>();
        try {
            for (URL provideLib : connectors) {
                org.apache.flink.core.fs.Path provideLibPath =
                        new org.apache.flink.core.fs.Path(provideLib.toURI());
                FileSystem fs = provideLibPath.getFileSystem();
                if (!fs.exists(provideLibPath)) {
                    LOGGER.info("provide remote connectors not exist, path: {}", provideLib);
                    continue;
                }
                retrieveFiles(fs, provideLibPath, this.connectors);
            }
        } catch (URISyntaxException | IOException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        Config envConfig = config.getConfig("env");
        registerPlugin(DiscoveryType.REMOTE, envConfig);
    }

    private static void retrieveFiles(
            FileSystem fs, org.apache.flink.core.fs.Path path, List<URL> files) throws IOException {
        if (!fs.exists(path)) {
            return;
        }
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDir()) {
                retrieveFiles(fs, fileStatus.getPath(), files);
                continue;
            }
            if (fileStatus.getPath().getName().endsWith("jar")) {
                URL fileUrl = fileStatus.getPath().makeQualified(fs).toUri().toURL();
                LOGGER.info("provide library, url: {}", fileUrl);
                files.add(fileUrl);
            }
        }
    }

    private void registerPlugin(DiscoveryType type, Config envConfig) {
        switch (type) {
            case LOCAL:
                registerLocalPlugin(envConfig);
                break;
            case REMOTE:
                registerRemotePlugin(envConfig);
                break;
            default:
                throw new SeaTunnelException("unsupported discovery type: " + type);
        }
    }

    private void registerRemotePlugin(Config envConfig) {
        if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
            actualUsedJars.addAll(
                    Arrays.stream(envConfig.getString(EnvCommonOptions.JARS.key()).split(";"))
                            .map(ResourceUtils::of)
                            .peek(
                                    url -> {
                                        try {
                                            ClassLoader classLoader =
                                                    Thread.currentThread().getContextClassLoader();
                                            ADD_URL_TO_CLASSLOADER.accept(classLoader, url);
                                        } catch (Exception e) {
                                            throw new SeaTunnelException(
                                                    "failed load user jar, path: " + url, e);
                                        }
                                    })
                            .collect(Collectors.toList()));
        }
    }

    private void registerLocalPlugin(Config envConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
            thirdPartyJars =
                    new ArrayList<>(
                            Common.getThirdPartyJars(
                                    envConfig.getString(EnvCommonOptions.JARS.key())));
        }
        thirdPartyJars.addAll(Common.getPluginsJarDependencies());
        List<URL> jarDependencies =
                Stream.concat(thirdPartyJars.stream(), Common.getLibJars().stream())
                        .map(Path::toUri)
                        .map(
                                uri -> {
                                    try {
                                        return uri.toURL();
                                    } catch (MalformedURLException e) {
                                        throw new RuntimeException(
                                                "the uri of jar illegal:" + uri, e);
                                    }
                                })
                        .collect(Collectors.toList());
        jarDependencies.forEach(
                url ->
                        ADD_URL_TO_CLASSLOADER.accept(
                                Thread.currentThread().getContextClassLoader(), url));
        actualUsedJars.addAll(jarDependencies);
    }

    private Config registerPlugin(DiscoveryType type, Config config, List<URL> jars) {
        config =
                this.injectJarsToConfig(
                        type, config, ConfigUtil.joinPath("env", "pipeline", "jars"), jars);
        return this.injectJarsToConfig(
                type, config, ConfigUtil.joinPath("env", "pipeline", "classpaths"), jars);
    }

    private Config injectJarsToConfig(
            DiscoveryType type, Config config, String path, List<URL> jars) {
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            switch (type) {
                case LOCAL:
                    checkLocalFile(jarUrl, validJars);
                    break;
                case REMOTE:
                    checkRemoteFile(jarUrl, validJars);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported discovery type: " + type);
            }
        }

        if (config.hasPath(path)) {
            Set<URL> paths =
                    Arrays.stream(config.getString(path).split(";"))
                            .map(
                                    uri -> {
                                        try {
                                            return new URL(uri);
                                        } catch (MalformedURLException e) {
                                            throw new RuntimeException(
                                                    "the uri of jar illegal:" + uri, e);
                                        }
                                    })
                            .collect(Collectors.toSet());
            paths.addAll(validJars);

            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    paths.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));

        } else {
            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    validJars.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));
        }
        return config;
    }

    private static void checkLocalFile(URL jarUrl, List<URL> validJars) {
        if (new File(jarUrl.getFile()).exists()) {
            validJars.add(jarUrl);
            LOGGER.info("Inject jar to config: {}", jarUrl);
        } else {
            LOGGER.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
        }
    }

    private static void checkRemoteFile(URL jarUrl, List<URL> validJars) {
        try {
            URI uri = jarUrl.toURI();
            FileSystem fs = FileSystem.get(uri);
            if (fs.exists(new org.apache.flink.core.fs.Path(uri))) {
                validJars.add(jarUrl);
                LOGGER.info("Inject jar to config: {}", jarUrl);
            }
        } catch (URISyntaxException | IOException e) {
            LOGGER.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
        }
    }
}
