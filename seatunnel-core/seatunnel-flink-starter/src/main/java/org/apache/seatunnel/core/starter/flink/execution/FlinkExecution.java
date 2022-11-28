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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.core.starter.flink.FlinkStarter;
import org.apache.seatunnel.core.starter.flink.config.FlinkCommon;
import org.apache.seatunnel.core.starter.flink.config.FlinkEnvironmentFactory;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to execute a SeaTunnelTask.
 */

@Slf4j
public class FlinkExecution implements TaskExecution {

    private final FlinkEnvironment flinkEnvironment;
    private final PluginExecuteProcessor sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor transformPluginExecuteProcessor;
    private final PluginExecuteProcessor sinkPluginExecuteProcessor;
    private final List<URL> jarPaths;

    public FlinkExecution(Config config) {
        try {
            jarPaths = new ArrayList<>(Collections.singletonList(
                new File(Common.appStarterDir().resolve(FlinkStarter.APP_JAR_NAME).toString()).toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        registerPlugin(config.getConfig("env"));
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(FlinkEnvironmentFactory.getJobMode(config));

        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(jarPaths, config.getConfigList(Constants.SOURCE), jobContext);
        this.transformPluginExecuteProcessor = new TransformExecuteProcessor(jarPaths,
            TypesafeConfigUtils.getConfigList(config, Constants.TRANSFORM, Collections.emptyList()), jobContext);
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(jarPaths, config.getConfigList(Constants.SINK), jobContext);

        this.flinkEnvironment = new FlinkEnvironmentFactory(this.registerPlugin(config, jarPaths)).getEnvironment();

        this.sourcePluginExecuteProcessor.setFlinkEnvironment(flinkEnvironment);
        this.transformPluginExecuteProcessor.setFlinkEnvironment(flinkEnvironment);
        this.sinkPluginExecuteProcessor.setFlinkEnvironment(flinkEnvironment);
    }

    @Override
    public void execute() throws TaskExecuteException {
        List<DataStream<Row>> dataStreams = new ArrayList<>();
        dataStreams = sourcePluginExecuteProcessor.execute(dataStreams);
        dataStreams = transformPluginExecuteProcessor.execute(dataStreams);
        sinkPluginExecuteProcessor.execute(dataStreams);

        log.info("Flink Execution Plan:{}", flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
        try {
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            throw new TaskExecuteException("Execute Flink job error", e);
        }
    }

    private void registerPlugin(Config envConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
            thirdPartyJars = new ArrayList<>(Common.getThirdPartyJars(envConfig.getString(EnvCommonOptions.JARS.key())));
        }
        thirdPartyJars.addAll(Common.getPluginsJarDependencies());
        List<URL> jarDependencies = Stream.concat(thirdPartyJars.stream(), Common.getLibJars().stream())
            .map(Path::toUri)
            .map(uri -> {
                try {
                    return uri.toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException("the uri of jar illegal:" + uri, e);
                }
            })
            .collect(Collectors.toList());
        jarDependencies.forEach(url -> FlinkCommon.ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url));

        jarPaths.addAll(jarDependencies);
    }

    private Config registerPlugin(Config config, List<URL> jars) {
        config = this.injectJarsToConfig(config, ConfigUtil.joinPath("env", "pipeline", "jars"), jars);
        return this.injectJarsToConfig(config, ConfigUtil.joinPath("env", "pipeline", "classpaths"), jars);
    }

    private Config injectJarsToConfig(Config config, String path, List<URL> jars) {
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            if (new File(jarUrl.getFile()).exists()) {
                validJars.add(jarUrl);
                log.info("Inject jar to config: {}", jarUrl);
            } else {
                log.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
            }
        }

        if (config.hasPath(path)) {
            Set<URL> paths = Arrays.stream(config.getString(path).split(";")).map(uri -> {
                try {
                    return new URL(uri);
                } catch (MalformedURLException e) {
                    throw new RuntimeException("the uri of jar illegal:" + uri, e);
                }
            }).collect(Collectors.toSet());
            paths.addAll(validJars);

            config = config.withValue(path,
                ConfigValueFactory.fromAnyRef(paths.stream().map(URL::toString).distinct().collect(Collectors.joining(";"))));

        } else {
            config = config.withValue(path,
                ConfigValueFactory.fromAnyRef(validJars.stream().map(URL::toString).distinct().collect(Collectors.joining(";"))));
        }
        return config;
    }
}
