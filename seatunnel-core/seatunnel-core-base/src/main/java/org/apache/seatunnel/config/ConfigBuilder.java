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

import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchExecution;
import org.apache.seatunnel.flink.stream.FlinkStreamExecution;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchExecution;
import org.apache.seatunnel.spark.stream.SparkStreamingExecution;
import org.apache.seatunnel.utils.Engine;
import org.apache.seatunnel.utils.PluginType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class ConfigBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBuilder.class);

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final String configFile;
    private final Engine engine;
    private ConfigPackage configPackage;
    private final Config config;
    private boolean streaming;
    private Config envConfig;
    private final RuntimeEnv env;

    public ConfigBuilder(String configFile, Engine engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.config = load();
        this.env = createEnv();
        this.configPackage = new ConfigPackage(engine.getEngine());
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
     * create plugin class instance, ignore case.
     **/
    private <T extends Plugin<?>> T createPluginInstanceIgnoreCase(String name, PluginType pluginType) throws Exception {
        if (name.split("\\.").length != 1) {
            // canonical class name
            return (T) Class.forName(name).newInstance();
        }
        String packageName;
        ServiceLoader<T> plugins;
        switch (pluginType) {
            case SOURCE:
                packageName = configPackage.getSourcePackage();
                Class<T> baseSource = (Class<T>) Class.forName(configPackage.getBaseSourceClass());
                plugins = ServiceLoader.load(baseSource);
                break;
            case TRANSFORM:
                packageName = configPackage.getTransformPackage();
                Class<T> baseTransform = (Class<T>) Class.forName(configPackage.getBaseTransformClass());
                plugins = ServiceLoader.load(baseTransform);
                break;
            case SINK:
                packageName = configPackage.getSinkPackage();
                Class<T> baseSink = (Class<T>) Class.forName(configPackage.getBaseSinkClass());
                plugins = ServiceLoader.load(baseSink);
                break;
            default:
                throw new IllegalArgumentException("PluginType not support : [" + pluginType + "]");
        }
        String canonicalName = packageName + "." + name;
        for (Iterator<T> it = plugins.iterator(); it.hasNext(); ) {
            try {
                T plugin = it.next();
                Class<?> serviceClass = plugin.getClass();
                String serviceClassName = serviceClass.getName();
                String clsNameToLower = serviceClassName.toLowerCase();
                if (clsNameToLower.equals(canonicalName.toLowerCase())) {
                    return plugin;
                }
            } catch (ServiceConfigurationError e) {
                // Iterator.next() may throw ServiceConfigurationError,
                // but maybe caused by a not used plugin in this job
                LOGGER.warn("Error when load plugin: [{}]", canonicalName, e);
            }
        }
        throw new ClassNotFoundException("Plugin class not found by name :[" + canonicalName + "]");
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

    public <T extends Plugin<?>> List<T> createPlugins(PluginType type) {
        Objects.requireNonNull(type, "PluginType can not be null when create plugins!");
        List<T> basePluginList = new ArrayList<>();
        List<? extends Config> configList = config.getConfigList(type.getType());
        configList.forEach(plugin -> {
            try {
                T t = createPluginInstanceIgnoreCase(plugin.getString(PLUGIN_NAME_KEY), type);
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
                throw new IllegalArgumentException("Engine: " + engine + " is not supported");
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
