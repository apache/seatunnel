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

package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchExecution;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamExecution;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.spark.SparkEnvironment;
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchExecution;
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingExecution;
import io.github.interestinglab.waterdrop.utils.Engine;
import io.github.interestinglab.waterdrop.utils.PluginType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ConfigBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBuilder.class);

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private String configFile;
    private Engine engine;
    private ConfigPackage configPackage;
    private Config config;
    private boolean streaming;
    private Config envConfig;
    private RuntimeEnv env;

    public ConfigBuilder(String configFile, Engine engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.configPackage = new ConfigPackage(engine.getEngine());
        this.config = load();
        this.env = createEnv();
    }

    public ConfigBuilder(String configFile) {
        this.configFile = configFile;
        this.engine = Engine.NULL;
        this.config = load();
        this.env = createEnv();
    }

    private Config load() {

        if (configFile.isEmpty()) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        LOGGER.info("Loading config file: {}", configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
                .parseFile(new File(configFile))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        LOGGER.info("parsed config file: {}", config.root().render(options));
        return config;
    }

    public Config getEnvConfigs() {
        return envConfig;
    }

    public RuntimeEnv getEnv() {
        return env;
    }

    private boolean checkIsStreaming() {
        List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());

        return sourceConfigList.get(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream");
    }

    /**
     * Get full qualified class name by reflection api, ignore case.
     **/
    private String buildClassFullQualifier(String name, PluginType classType) throws Exception {

        if (name.split("\\.").length == 1) {
            String packageName = null;
            Iterable<? extends Plugin> plugins = null;
            switch (classType) {
                case SOURCE:
                    packageName = configPackage.sourcePackage();
                    Class baseSource = Class.forName(configPackage.baseSourcePackage());
                    plugins = ServiceLoader.load(baseSource);
                    break;
                case TRANSFORM:
                    packageName = configPackage.transformPackage();
                    Class baseTransform = Class.forName(configPackage.baseTransformPackage());
                    plugins = ServiceLoader.load(baseTransform);
                    break;
                case SINK:
                    packageName = configPackage.sinkPackage();
                    Class baseSink = Class.forName(configPackage.baseSinkPackage());
                    plugins = ServiceLoader.load(baseSink);
                    break;
                default:
                    break;
            }

            String qualifierWithPackage = packageName + "." + name;
            for (Plugin plugin : plugins) {
                Class serviceClass = plugin.getClass();
                String serviceClassName = serviceClass.getName();
                String clsNameToLower = serviceClassName.toLowerCase();
                if (clsNameToLower.equals(qualifierWithPackage.toLowerCase())) {
                    return serviceClassName;
                }
            }
            return qualifierWithPackage;
        } else {
            return name;
        }
    }


    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.createPlugins(PluginType.SOURCE);
        this.createPlugins(PluginType.TRANSFORM);
        this.createPlugins(PluginType.SINK);
    }

    public <T extends Plugin> List<T> createPlugins(PluginType type) {

        List<T> basePluginList = new ArrayList<>();

        List<? extends Config> configList = config.getConfigList(type.getType());

        configList.forEach(plugin -> {
            try {
                final String className = buildClassFullQualifier(plugin.getString(PLUGIN_NAME_KEY), type);
                T t = (T) Class.forName(className).newInstance();
                t.setConfig(plugin);
                basePluginList.add(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return basePluginList;
    }

    private RuntimeEnv createEnv() {
        envConfig = config.getConfig("env");
        streaming = checkIsStreaming();
        RuntimeEnv env = null;
        switch (engine) {
            case SPARK:
                env = new SparkEnvironment();
                break;
            case FLINK:
                env = new FlinkEnvironment();
                break;
            default:
                break;
        }
        env.setConfig(envConfig);
        env.prepare(streaming);
        return env;
    }

    public Execution createExecution() {
        Execution execution = null;
        switch (engine) {
            case SPARK:
                SparkEnvironment sparkEnvironment = (SparkEnvironment) env;
                if (streaming) {
                    execution = new SparkStreamingExecution(sparkEnvironment);
                } else {
                    execution = new SparkBatchExecution(sparkEnvironment);
                }
                break;
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) env;
                if (streaming) {
                    execution = new FlinkStreamExecution(flinkEnvironment);
                } else {
                    execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                break;
        }
        return execution;
    }

}
