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

package org.apache.seatunnel.command;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.utils.AsciiArtUtils;
import org.apache.seatunnel.utils.CompressionUtils;

import org.apache.commons.compress.archivers.ArchiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Base task execute command. More details see:
 * <ul>
 *     <li>{@link org.apache.seatunnel.command.flink.FlinkTaskExecuteCommand}</li>
 *     <li>{@link org.apache.seatunnel.command.spark.SparkTaskExecuteCommand}</li>
 * </ul>
 *
 * @param <T> command args.
 */
public abstract class BaseTaskExecuteCommand<T extends CommandArgs> implements Command<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskExecuteCommand.class);

    /**
     * Check the plugin config.
     *
     * @param plugins plugin list.
     */
    protected void baseCheckConfig(List<? extends Plugin>... plugins) {
        pluginCheck(plugins);
        deployModeCheck();
    }

    /**
     * Execute prepare method defined in {@link Plugin}.
     *
     * @param env runtimeEnv
     * @param plugins plugin list
     */
    protected void prepare(RuntimeEnv env, List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }
    }

    /**
     * Print the logo.
     */
    protected void showAsciiLogo() {
        String printAsciiLogo = System.getenv("SEATUNNEL_PRINT_ASCII_LOGO");
        if ("true".equalsIgnoreCase(printAsciiLogo)) {
            AsciiArtUtils.printAsciiArt(Constants.LOGO);
        }
    }

    /**
     * Execute the checkConfig method defined in {@link Plugin}.
     *
     * @param plugins plugin list
     */
    private void pluginCheck(List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            for (Plugin plugin : pluginList) {
                CheckResult checkResult;
                try {
                    checkResult = plugin.checkConfig();
                } catch (Exception e) {
                    checkResult = CheckResult.error(e.getMessage());
                }
                if (!checkResult.isSuccess()) {
                    LOGGER.error("Plugin[{}] contains invalid config, error: {} \n", plugin.getClass().getName(), checkResult.getMsg());
                    System.exit(-1); // invalid configuration
                }
            }
        }
    }

    private void deployModeCheck() {
        final Optional<String> mode = Common.getDeployMode();
        if (mode.isPresent() && DeployMode.CLUSTER.getName().equals(mode.get())) {

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

}
