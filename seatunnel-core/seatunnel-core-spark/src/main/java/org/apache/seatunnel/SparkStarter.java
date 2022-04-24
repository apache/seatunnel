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

package org.apache.seatunnel;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

import org.apache.seatunnel.command.SparkCommandArgs;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.config.ConfigBuilder;
import org.apache.seatunnel.config.EngineType;
import org.apache.seatunnel.config.PluginFactory;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.utils.CompressionUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Starter to generate spark-submit command for SeaTunnel job on spark.
 */
public class SparkStarter implements Starter {

    private static final int USAGE_EXIT_CODE = 234;

    private static final int PLUGIN_LIB_DIR_DEPTH = 3;

    /**
     * original commandline args
     */
    protected String[] args;

    /**
     * args parsed from {@link #args}
     */
    protected SparkCommandArgs commandArgs;

    /**
     * the spark application name
     */
    protected String appName;

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
        SparkCommandArgs commandArgs = parseCommandArgs(args);
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

    /**
     * parse commandline args
     */
    private static SparkCommandArgs parseCommandArgs(String[] args) {
        SparkCommandArgs commandArgs = new SparkCommandArgs();
        JCommander commander = JCommander.newBuilder()
            .programName("start-seatunnel-spark.sh")
            .addObject(commandArgs)
            .args(args)
            .build();
        if (commandArgs.isHelp()) {
            commander.usage();
            System.exit(USAGE_EXIT_CODE);
        }
        return commandArgs;
    }

    @Override
    public List<String> buildCommands() throws IOException {
        setSparkConf();
        Common.setDeployMode(commandArgs.getDeployMode().getName());
        this.jars.addAll(getPluginsJarDependencies());
        this.jars.addAll(listJars(Common.appLibDir()));
        this.jars.addAll(getConnectorJarDependencies());
        this.appName = this.sparkConf.getOrDefault("spark.app.name", Constants.LOGO);
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
        String driverJavaOpts = this.sparkConf.get("spark.driver.extraJavaOptions");
        String executorJavaOpts = this.sparkConf.get("spark.executor.extraJavaOptions");
        if (!commandArgs.getVariables().isEmpty()) {
            String properties = commandArgs.getVariables()
                .stream()
                .map(v -> "-D" + v)
                .collect(Collectors.joining(" "));
            driverJavaOpts += " " + properties;
            executorJavaOpts += " " + properties;
            this.sparkConf.put("spark.driver.extraJavaOptions", driverJavaOpts);
            this.sparkConf.put("spark.executor.extraJavaOptions", executorJavaOpts);
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
     * return plugin's dependent jars, which located in 'plugins/${pluginName}/lib/*'.
     */
    private List<Path> getPluginsJarDependencies() throws IOException {
        Path pluginRootDir = Common.pluginRootDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(pluginRootDir, PLUGIN_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream
                .filter(it -> pluginRootDir.relativize(it).getNameCount() == PLUGIN_LIB_DIR_DEPTH)
                .filter(it -> it.getParent().endsWith("lib"))
                .filter(it -> it.getFileName().endsWith("jar"))
                .collect(Collectors.toList());
        }
    }

    /**
     * return connector's jars, which located in 'connectors/spark/*'.
     */
    private List<Path> getConnectorJarDependencies() {
        Path pluginRootDir = Common.connectorRootDir("SPARK");
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        Config config = new ConfigBuilder<>(commandArgs.getConfigFile(), EngineType.SPARK).getConfig();
        PluginFactory<RuntimeEnv> pluginFactory = new PluginFactory<>(config, EngineType.SPARK);
        return pluginFactory.getPluginJarPaths().stream().map(url -> new File(url.getPath()).toPath()).collect(Collectors.toList());
    }

    /**
     * list jars in given directory
     */
    private List<Path> listJars(Path dir) throws IOException {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream
                    .filter(it -> !Files.isDirectory(it))
                    .filter(it -> it.getFileName().endsWith("jar"))
                    .collect(Collectors.toList());
        }
    }

    /**
     * build final spark-submit commands
     */
    protected List<String> buildFinal() {
        List<String> commands = new ArrayList<>();
        commands.add("${SPARK_HOME}/bin/spark-submit");
        appendOption(commands, "--class", "org.apache.seatunnel.SeatunnelSpark");
        appendOption(commands, "--name", this.appName);
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getName());
        appendJars(commands, this.jars);
        appendFiles(commands, this.files);
        appendSparkConf(commands, this.sparkConf);
        appendAppJar(commands);
        appendArgs(commands, args);
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
     * append original commandline args to StringBuilder
     */
    protected void appendArgs(List<String> commands, String[] args) {
        commands.addAll(Arrays.asList(args));
    }

    /**
     * append appJar to StringBuilder
     */
    protected void appendAppJar(List<String> commands) {
        commands.add(Common.appLibDir().resolve("seatunnel-core-spark.jar").toString());
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
            Common.setDeployMode(commandArgs.getDeployMode().getName());
            Path pluginTarball = Common.pluginTarball();
            if (Files.notExists(pluginTarball)) {
                CompressionUtils.tarGzip(Common.pluginRootDir(), pluginTarball);
            }
            this.files.add(pluginTarball);
            this.files.add(Paths.get(commandArgs.getConfigFile()));
            return super.buildCommands();
        }
    }
}
