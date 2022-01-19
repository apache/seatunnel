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

import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.apis.BaseSource;
import org.apache.seatunnel.apis.BaseTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.config.ConfigBuilder;
import org.apache.seatunnel.config.command.CommandLineArgs;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.utils.AsciiArtUtils;
import org.apache.seatunnel.utils.CompressionUtils;
import org.apache.seatunnel.utils.Engine;
import org.apache.seatunnel.utils.PluginType;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Seatunnel {
    private static final Logger LOGGER = LoggerFactory.getLogger(Seatunnel.class);

    public static void run(CommandLineArgs commandLineArgs, Engine engine, String[] args) {
        Common.setDeployMode(commandLineArgs.getDeployMode());
        String configFilePath = getConfigFilePath(commandLineArgs, engine);
        boolean testConfig = commandLineArgs.isTestConfig();
        if (testConfig) {
            new ConfigBuilder(configFilePath, engine).checkConfig();
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

    private static String getConfigFilePath(CommandLineArgs cmdArgs, Engine engine) {
        String path = null;
        switch (engine) {
            case FLINK:
                path = cmdArgs.getConfigFile();
                break;
            case SPARK:
                final Optional<String> mode = Common.getDeployMode();
                if (mode.isPresent() && "cluster".equals(mode.get())) {
                    path = Paths.get(cmdArgs.getConfigFile()).getFileName().toString();
                } else {
                    path = cmdArgs.getConfigFile();
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
        String printAsciiLogo = System.getenv("SEATUNNEL_PRINT_ASCII_LOGO");
        if ("true".equalsIgnoreCase(printAsciiLogo)) {
            AsciiArtUtils.printAsciiArt(Constants.LOGO);
        }
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
            "Please submit bug report in https://github.com/apache/incubator-seatunnel/issues\n");
        LOGGER.error("Reason:{} \n", errorMsg);
        LOGGER.error("Exception StackTrace:{} ", ExceptionUtils.getStackTrace(throwable));
        LOGGER.error(
            "\n===============================================================================\n\n\n");
    }
}
