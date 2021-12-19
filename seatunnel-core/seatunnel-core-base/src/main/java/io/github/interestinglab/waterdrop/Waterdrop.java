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

package io.github.interestinglab.waterdrop;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.config.CommandLineArgs;
import io.github.interestinglab.waterdrop.common.config.Common;
import io.github.interestinglab.waterdrop.config.ConfigBuilder;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.utils.AsciiArtUtils;
import io.github.interestinglab.waterdrop.utils.CompressionUtils;
import io.github.interestinglab.waterdrop.utils.Engine;
import io.github.interestinglab.waterdrop.utils.PluginType;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scopt.OptionParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Waterdrop {
    private static final Logger LOGGER = LoggerFactory.getLogger(Waterdrop.class);

    public static void run(OptionParser<CommandLineArgs> parser, Engine engine, String[] args) {
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(args).iterator()).asScala().toSeq();
        Option<CommandLineArgs> option = parser.parse(seq, new CommandLineArgs("client", "application.conf", false));
        if (option.isDefined()) {
            CommandLineArgs commandLineArgs = option.get();
            Common.setDeployMode(commandLineArgs.deployMode());
            String configFilePath = getConfigFilePath(commandLineArgs, engine);
            boolean testConfig = commandLineArgs.testConfig();
            if (testConfig) {
                new ConfigBuilder(configFilePath).checkConfig();
                LOGGER.info("config OK !");
            } else {
                try {
                    entryPoint(configFilePath, engine);
                } catch (ConfigRuntimeException e) {
                    showConfigError(e);
                    throw e;
                } catch (Exception e) {
                    showFatalError(e);
                    throw e;
                }
            }
        }
    }

    private static String getConfigFilePath(CommandLineArgs cmdArgs, Engine engine) {
        String path = null;
        switch (engine) {
            case FLINK:
                path = cmdArgs.configFile();
                break;
            case SPARK:
                final Optional<String> mode = Common.getDeployMode();
                if (mode.isPresent() && "cluster".equals(mode.get())) {
                    path = Paths.get(cmdArgs.configFile()).getFileName().toString();
                } else {
                    path = cmdArgs.configFile();
                }
                break;
            default:
                break;
        }
        return path;
    }

    private static void entryPoint(String configFile, Engine engine) {

        ConfigBuilder configBuilder = new ConfigBuilder(configFile, engine);
        List<BaseSource> sources = configBuilder.createPlugins(PluginType.SOURCE);
        List<BaseTransform> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
        List<BaseSink> sinks = configBuilder.createPlugins(PluginType.SINK);
        Execution execution = configBuilder.createExecution();
        baseCheckConfig(sources, transforms, sinks);
        prepare(configBuilder.getEnv(), sources, transforms, sinks);
        showAsciiLogo();

        execution.start(sources, transforms, sinks);
    }

    private static void baseCheckConfig(List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            for (Plugin plugin : pluginList) {
                CheckResult checkResult = null;
                try {
                    checkResult = plugin.checkConfig();
                } catch (Exception e) {
                    checkResult = new CheckResult(false, e.getMessage());
                }
                if (!checkResult.isSuccess()) {
                    LOGGER.error("Plugin[{}] contains invalid config, error: {} \n", plugin.getClass().getName(), checkResult.getMsg());
                    System.exit(-1); // invalid configuration
                }
            }
        }
        deployModeCheck();
    }

    private static void deployModeCheck() {
        final Optional<String> mode = Common.getDeployMode();
        if (mode.isPresent() && "cluster".equals(mode.get())) {

            LOGGER.info("preparing cluster mode work dir files...");
            File workDir = new File(".");

            for (File file : Objects.requireNonNull(workDir.listFiles())) {
                LOGGER.warn("\t list file: " + file.getAbsolutePath());
            }
            // decompress plugin dir
            File compressedFile = new File("plugins.tar.gz");

            try {
                File tempFile = CompressionUtils.unGzip(compressedFile, workDir);
                try {
                    CompressionUtils.unTar(tempFile, workDir);
                    LOGGER.info("succeeded to decompress plugins.tar.gz");
                } catch (ArchiveException e) {
                    LOGGER.error("failed to decompress plugins.tar.gz", e);
                    System.exit(-1);
                }
            } catch (IOException e) {
                LOGGER.error("failed to decompress plugins.tar.gz", e);
                System.exit(-1);
            }
        }
    }

    private static void prepare(RuntimeEnv env, List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }

    }

    private static void showAsciiLogo() {
        AsciiArtUtils.printAsciiArt("SeaTunnel");
    }

    private static void showConfigError(Throwable throwable) {
        LOGGER.error(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        LOGGER.error("Config Error:\n");
        LOGGER.error("Reason: {} \n", errorMsg);
        LOGGER.error(
                "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        LOGGER.error(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        LOGGER.error("Fatal Error, \n");
        // FIX
        LOGGER.error(
                "Please submit issue a bug in https://github.com/InterestingLab/waterdrop/issues\n");
        LOGGER.error("Reason:{} \n", errorMsg);
        LOGGER.error("Exception StackTrace:{} ", ExceptionUtils.getStackTrace(throwable));
        LOGGER.error(
                "\n===============================================================================\n\n\n");
    }
}
