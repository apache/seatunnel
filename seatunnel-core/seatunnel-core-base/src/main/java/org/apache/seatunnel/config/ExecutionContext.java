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

package org.apache.seatunnel.config;

import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.apis.BaseSource;
import org.apache.seatunnel.apis.BaseTransform;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.env.RuntimeEnv;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;

/**
 * The ExecutionContext contains all configuration needed to run the job.
 *
 * @param <ENVIRONMENT> environment type.
 */
public class ExecutionContext<ENVIRONMENT extends RuntimeEnv> {

    private final Config config;
    private final EngineType engine;

    private final ENVIRONMENT environment;
    private final JobMode jobMode;
    private final List<BaseSource<ENVIRONMENT>> sources;
    private final List<BaseTransform<ENVIRONMENT>> transforms;
    private final List<BaseSink<ENVIRONMENT>> sinks;

    public ExecutionContext(Config config, EngineType engine) {
        this.config = config;
        this.engine = engine;
        this.environment = new EnvironmentFactory<ENVIRONMENT>(config, engine).getEnvironment();
        this.jobMode = environment.getJobMode();
        PluginFactory<ENVIRONMENT> pluginFactory = new PluginFactory<>(config, engine);
        this.environment.registerPlugin(pluginFactory.getPluginJarPaths());
        this.sources = pluginFactory.createPlugins(PluginType.SOURCE);
        this.transforms = pluginFactory.createPlugins(PluginType.TRANSFORM);
        this.sinks = pluginFactory.createPlugins(PluginType.SINK);
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

    public List<BaseSource<ENVIRONMENT>> getSources() {
        return sources;
    }

    public List<BaseTransform<ENVIRONMENT>> getTransforms() {
        return transforms;
    }

    public List<BaseSink<ENVIRONMENT>> getSinks() {
        return sinks;
    }
}
