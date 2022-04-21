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
import org.apache.seatunnel.plugin.PluginClosedException;
import org.apache.seatunnel.utils.AsciiArtUtils;
import org.apache.seatunnel.utils.CompressionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
public abstract class BaseTaskExecuteCommand<T extends AbstractCommandArgs, E extends RuntimeEnv> implements Command<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskExecuteCommand.class);

    /**
     * Check the plugin config.
     *
     * @param plugins plugin list.
     */
    @SafeVarargs
    protected final void baseCheckConfig(List<? extends Plugin<E>>... plugins) {
        pluginCheck(plugins);
        deployModeCheck();
    }

    /**
     * Execute prepare method defined in {@link org.apache.seatunnel.plugin.Plugin}.
     *
     * @param env     runtimeEnv
     * @param plugins plugin list
     */
    @SafeVarargs
    protected final void prepare(E env, List<? extends Plugin<E>>... plugins) {
        for (List<? extends Plugin<E>> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }
    }

    /**
     * Execute close method defined in {@link org.apache.seatunnel.plugin.Plugin}
     *
     * @param plugins plugin list
     */
    @SafeVarargs
    protected final void close(List<? extends Plugin<E>>... plugins) {
        PluginClosedException exceptionHolder = null;
        for (List<? extends Plugin<E>> pluginList : plugins) {
            for (Plugin<E> plugin : pluginList) {
                try (Plugin<?> closed = plugin) {
                    // ignore
                } catch (Exception e) {
                    exceptionHolder = exceptionHolder == null ?
                            new PluginClosedException("below plugins closed error:") : exceptionHolder;
                    exceptionHolder.addSuppressed(new PluginClosedException(
                            String.format("plugin %s closed error", plugin.getClass()), e));
                }
            }
        }
        if (exceptionHolder != null) {
            throw exceptionHolder;
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
    private void pluginCheck(List<? extends Plugin<E>>... plugins) {
        for (List<? extends Plugin<E>> pluginList : plugins) {
            for (Plugin<E> plugin : pluginList) {
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
                LOGGER.warn("\t list file: {} ", file.getAbsolutePath());
            }
            // decompress plugin dir
            File compressedFile = new File("plugins.tar.gz");

            try {
                File tempFile = CompressionUtils.unGzip(compressedFile, workDir);
                CompressionUtils.unTar(tempFile, workDir);
            } catch (Exception e) {
                LOGGER.error("failed to decompress plugins.tar.gz", e);
                System.exit(-1);
            }
            LOGGER.info("succeeded to decompress plugins.tar.gz");
        }
    }

}
