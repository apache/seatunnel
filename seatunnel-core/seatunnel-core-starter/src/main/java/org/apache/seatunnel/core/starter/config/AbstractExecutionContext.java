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

package org.apache.seatunnel.core.starter.config;

import org.apache.seatunnel.apis.base.api.BaseSink;
import org.apache.seatunnel.apis.base.api.BaseSource;
import org.apache.seatunnel.apis.base.api.BaseTransform;
import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The ExecutionContext contains all configuration needed to run the job.
 *
 * @param <ENVIRONMENT> environment type.
 */
public abstract class AbstractExecutionContext<ENVIRONMENT extends RuntimeEnv> {

    private final Config config;
    private final EngineType engine;

    private final ENVIRONMENT environment;
    private final JobMode jobMode;

    public AbstractExecutionContext(Config config, EngineType engine) {
        this.config = config;
        this.engine = engine;
        this.environment = new EnvironmentFactory<ENVIRONMENT>(config, engine).getEnvironment();
        this.jobMode = environment.getJobMode();
    }

    public Config getRootConfig() {
        return config;
    }

    public EngineType getEngine() {
        return engine;
    }

    public ENVIRONMENT getEnvironment() {
        return environment;
    }

    public JobMode getJobMode() {
        return jobMode;
    }

    public abstract List<BaseSource<ENVIRONMENT>> getSources();

    public abstract List<BaseTransform<ENVIRONMENT>> getTransforms();

    public abstract List<BaseSink<ENVIRONMENT>> getSinks();

    public abstract List<URL> getPluginJars();

    @SuppressWarnings("checkstyle:Indentation")
    protected List<PluginIdentifier> getPluginIdentifiers(PluginType... pluginTypes) {
        return Arrays.stream(pluginTypes).flatMap((Function<PluginType, Stream<PluginIdentifier>>) pluginType -> {
            List<? extends Config> configList = config.getConfigList(pluginType.getType());
            return configList.stream()
                .map(pluginConfig -> PluginIdentifier
                    .of(engine.getEngine(),
                        pluginType.getType(),
                        pluginConfig.getString("plugin_name")));
        }).collect(Collectors.toList());
    }
}
