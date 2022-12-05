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

package org.apache.seatunnel.core.starter.spark;

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.Starter;
import org.apache.seatunnel.core.starter.config.ConfigBuilder;
import org.apache.seatunnel.core.starter.config.PluginType;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.spark.config.StarterConstant;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;
import org.apache.seatunnel.core.starter.utils.CompressionUtils;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Starter to generate spark-submit command for SeaTunnel job on spark.
 */
public class SparkStarter implements Starter {

    /**
     * original commandline args
     */
    protected String[] args;

    /**
     * args parsed from {@link #args}
     */
    protected SparkCommandArgs commandArgs;

    /**
     * jars to include on the spark driver and executor classpaths
     */
    protected List<Path> jars = new ArrayList<>();

    /**
     * files to be placed in the working directory of each spark executor
     */
    protected List<Path> files = new ArrayList<>();

    /**
     * spark configuration properties
     */
    protected Map<String, String> sparkConf;

    private SparkStarter(String[] args, SparkCommandArgs commandArgs) {
        this.args = args;
        this.commandArgs = commandArgs;
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) throws IOException {
        SparkStarter starter = getInstance(args);
        List<String> command = starter.buildCommands();
        System.out.println(String.join(" ", command));
    }

    /**
     * method to get SparkStarter instance, will return
     * {@link ClusterModeSparkStarter} or
     * {@link ClientModeSparkStarter} depending on deploy mode.
     */
    static SparkStarter getInstance(String[] args) {
        SparkCommandArgs commandArgs = CommandLineUtils.parse(args, new SparkCommandArgs(), StarterConstant.SHELL_NAME, true);
        DeployMode deployMode = commandArgs.getDeployMode();
        switch (deployMode) {
            case CLUSTER:
                return new ClusterModeSparkStarter(args, commandArgs);
            case CLIENT:
                return new ClientModeSparkStarter(args, commandArgs);
            default:
                throw new IllegalArgumentException("Deploy mode " + deployMode + " not supported");
        }
    }

    @Override
    public List<String> buildCommands() throws IOException {
        setSparkConf();
        Common.setDeployMode(commandArgs.getDeployMode());
        Common.setStarter(true);
        this.jars.addAll(Common.getPluginsJarDependencies());
        this.jars.addAll(Common.getLibJars());
        this.jars.addAll(getConnectorJarDependencies());
        this.jars.addAll(new ArrayList<>(Common.getThirdPartyJars(sparkConf.getOrDefault(EnvCommonOptions.JARS.key(), ""))));
        return buildFinal();
    }

    /**
     * parse spark configurations from SeaTunnel config file
     */
    private void setSparkConf() throws FileNotFoundException {
        commandArgs.getVariables()
            .stream()
            .filter(Objects::nonNull)
            .map(variable -> variable.split("=", 2))
            .filter(pair -> pair.length == 2)
            .forEach(pair -> System.setProperty(pair[0], pair[1]));
        this.sparkConf = getSparkConf(commandArgs.getConfigFile());
        String driverJavaOpts = this.sparkConf.getOrDefault("spark.driver.extraJavaOptions", "");
        String executorJavaOpts = this.sparkConf.getOrDefault("spark.executor.extraJavaOptions", "");
        if (!commandArgs.getVariables().isEmpty()) {
            String properties = commandArgs.getVariables()
                .stream()
                .map(v -> "-D" + v)
                .collect(Collectors.joining(" "));
            driverJavaOpts += " " + properties;
            executorJavaOpts += " " + properties;
            this.sparkConf.put("spark.driver.extraJavaOptions", driverJavaOpts.trim());
            this.sparkConf.put("spark.executor.extraJavaOptions", executorJavaOpts.trim());
        }
    }

    /**
     * Get spark configurations from SeaTunnel job config file.
     */
    static Map<String, String> getSparkConf(String configFile) throws FileNotFoundException {
        File file = new File(configFile);
        if (!file.exists()) {
            throw new FileNotFoundException("config file '" + file + "' does not exists!");
        }
        Config appConfig = ConfigFactory.parseFile(file)
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        return appConfig.getConfig("env")
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().unwrapped().toString()));
    }

    /**
     * return connector's jars, which located in 'connectors/spark/*'.
     */
    private List<Path> getConnectorJarDependencies() {
        Path pluginRootDir = Common.connectorJarDir("seatunnel");
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        Config config = new ConfigBuilder(Paths.get(commandArgs.getConfigFile())).getConfig();
        Set<URL> pluginJars = new HashSet<>();
        SeaTunnelSourcePluginDiscovery seaTunnelSourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        SeaTunnelSinkPluginDiscovery seaTunnelSinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        pluginJars.addAll(seaTunnelSourcePluginDiscovery.getPluginJarPaths(getPluginIdentifiers(config, PluginType.SOURCE)));
        pluginJars.addAll(seaTunnelSinkPluginDiscovery.getPluginJarPaths(getPluginIdentifiers(config, PluginType.SINK)));
        return pluginJars.stream().map(url -> new File(url.getPath()).toPath()).collect(Collectors.toList());
    }

    /**
     * build final spark-submit commands
     */
    protected List<String> buildFinal() {
        List<String> commands = new ArrayList<>();
        commands.add("${SPARK_HOME}/bin/spark-submit");
        appendOption(commands, "--class", SeatunnelSpark.class.getName());
        appendOption(commands, "--name", this.commandArgs.getJobName());
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getName());
        appendJars(commands, this.jars);
        appendFiles(commands, this.files);
        appendSparkConf(commands, this.sparkConf);
        appendAppJar(commands);
        appendOption(commands, "--config", this.commandArgs.getConfigFile());
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getName());
        return commands;
    }

    /**
     * append option to StringBuilder
     */
    protected void appendOption(List<String> commands, String option, String value) {
        commands.add(option);
        commands.add("\"" + value.replace("\"", "\\\"") + "\"");
    }

    /**
     * append jars option to StringBuilder
     */
    protected void appendJars(List<String> commands, List<Path> paths) {
        appendPaths(commands, "--jars", paths);
    }

    /**
     * append files option to StringBuilder
     */
    protected void appendFiles(List<String> commands, List<Path> paths) {
        appendPaths(commands, "--files", paths);
    }

    /**
     * append comma-split paths option to StringBuilder
     */
    protected void appendPaths(List<String> commands, String option, List<Path> paths) {
        if (!paths.isEmpty()) {
            String values = paths.stream()
                .map(Path::toString)
                .collect(Collectors.joining(","));
            appendOption(commands, option, values);
        }
    }

    /**
     * append spark configurations to StringBuilder
     */
    protected void appendSparkConf(List<String> commands, Map<String, String> sparkConf) {
        for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            appendOption(commands, "--conf", key + "=" + value);
        }
    }

    /**
     * append appJar to StringBuilder
     */
    protected void appendAppJar(List<String> commands) {
        commands.add(Common.appStarterDir().resolve("seatunnel-spark-starter.jar").toString());
    }

    @SuppressWarnings("checkstyle:Indentation")
    private List<PluginIdentifier> getPluginIdentifiers(Config config, PluginType... pluginTypes) {
        return Arrays.stream(pluginTypes).flatMap((Function<PluginType, Stream<PluginIdentifier>>) pluginType -> {
            List<? extends Config> configList = config.getConfigList(pluginType.getType());
            return configList.stream()
                .map(pluginConfig -> PluginIdentifier.of("seatunnel", pluginType.getType(),
                    pluginConfig.getString("plugin_name")));
        }).collect(Collectors.toList());
    }

    /**
     * a Starter for building spark-submit commands with client mode options
     */
    private static class ClientModeSparkStarter extends SparkStarter {

        /**
         * client mode specified spark options
         */
        private enum ClientModeSparkConfigs {

            /**
             * Memory for driver in client mode
             */
            DriverMemory("--driver-memory", "spark.driver.memory"),

            /**
             * Extra Java options to pass to the driver in client mode
             */
            DriverJavaOptions("--driver-java-options", "spark.driver.extraJavaOptions"),

            /**
             * Extra library path entries to pass to the driver in client mode
             */
            DriverLibraryPath(" --driver-library-path", "spark.driver.extraLibraryPath"),

            /**
             * Extra class path entries to pass to the driver in client mode
             */
            DriverClassPath("--driver-class-path", "spark.driver.extraClassPath");

            private final String optionName;

            private final String propertyName;

            private static final Map<String, ClientModeSparkConfigs> PROPERTY_NAME_MAP = new HashMap<>();

            static {
                for (ClientModeSparkConfigs config : values()) {
                    PROPERTY_NAME_MAP.put(config.propertyName, config);
                }
            }

            ClientModeSparkConfigs(String optionName, String propertyName) {
                this.optionName = optionName;
                this.propertyName = propertyName;
            }
        }

        private ClientModeSparkStarter(String[] args, SparkCommandArgs commandArgs) {
            super(args, commandArgs);
        }

        @Override
        protected void appendSparkConf(List<String> commands, Map<String, String> sparkConf) {
            for (ClientModeSparkConfigs config : ClientModeSparkConfigs.values()) {
                String driverJavaOptions = this.sparkConf.get(config.propertyName);
                if (StringUtils.isNotBlank(driverJavaOptions)) {
                    appendOption(commands, config.optionName, driverJavaOptions);
                }
            }
            for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (ClientModeSparkConfigs.PROPERTY_NAME_MAP.containsKey(key)) {
                    continue;
                }
                appendOption(commands, "--conf", key + "=" + value);
            }
        }
    }

    /**
     * a Starter for building spark-submit commands with cluster mode options
     */
    private static class ClusterModeSparkStarter extends SparkStarter {

        private ClusterModeSparkStarter(String[] args, SparkCommandArgs commandArgs) {
            super(args, commandArgs);
        }

        @Override
        public List<String> buildCommands() throws IOException {
            Common.setDeployMode(commandArgs.getDeployMode());
            Common.setStarter(true);
            Path pluginTarball = Common.pluginTarball();
            CompressionUtils.tarGzip(Common.pluginRootDir(), pluginTarball);
            this.files.add(pluginTarball);
            this.files.add(Paths.get(commandArgs.getConfigFile()));
            return super.buildCommands();
        }
    }
}
