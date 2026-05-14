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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.api.options.EnvOptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.core.parse.ConfigParserUtil;
import org.apache.seatunnel.engine.core.parse.JobPluginClasspathHelper;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;

/**
 * Checks the job config file without running the job. Use {@code --check} or {@code
 * --dry-run=static}.
 *
 * <p>What gets checked:
 *
 * <ul>
 *   <li>Config file syntax (valid HOCON or YAML)
 *   <li>Each connector/transform plugin: name is known, required options are present, option types
 *       are correct, and unknown option keys are reported (helps catch typos)
 *   <li>Pipeline wiring: at least one source and one sink, and inputs/outputs connect sensibly
 *   <li>For the Sql transform: optional SQL text syntax check when the SQL parser library is on the
 *       classpath
 * </ul>
 *
 * <p>This does not submit a job and does not run the full "build the pipeline and talk to databases
 * or catalogs" step. SeaTunnel still loads plugin classes to read their option definitions; loading
 * code may touch disk or, in rare cases, the network during class startup, so treat this as offline
 * validation of the config file, not a strict "zero I/O" sandbox. Please note that this validation
 * service is provided exclusively via the Command Line Interface (CLI).
 *
 * <p>Plugin discovery and classloader creation follow the same contract as {@link
 * org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser}: source and transform
 * connectors share one child-first classloader, while sink connectors use a separate one. This
 * ensures that --dry-run static validates against the same factory set that the real runtime parser
 * would resolve.
 */
@Slf4j
public class SeaTunnelConfValidateCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    public SeaTunnelConfValidateCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @Override
    public void execute() throws ConfigCheckException {
        Path configPath = FileUtils.getConfigPath(clientCommandArgs);

        try {
            Config config = ConfigBuilder.of(configPath, clientCommandArgs.getVariables());

            if (config.hasPath("env")) {
                ReadonlyConfig envConfig = ReadonlyConfig.fromConfig(config.getConfig("env"));
                OptionRule envRule = new EnvOptionRule().optionRule();
                ConfigValidator.of(envConfig).validate(envRule);
                validateOptionTypes(envConfig, envRule);
                ConfigValidator.validateUnknownKeys(envConfig, envRule, "env");
            }

            List<? extends Config> sourceConfigs =
                    config.hasPath("source")
                            ? config.getConfigList("source")
                            : Collections.emptyList();
            List<? extends Config> transformConfigs =
                    config.hasPath("transform")
                            ? config.getConfigList("transform")
                            : Collections.emptyList();
            List<? extends Config> sinkConfigs =
                    config.hasPath("sink") ? config.getConfigList("sink") : Collections.emptyList();

            ReadonlyConfig envReadonly =
                    config.hasPath("env")
                            ? ReadonlyConfig.fromConfig(config.getConfig("env"))
                            : ReadonlyConfig.fromMap(Collections.emptyMap());
            List<URL> commonPluginJars =
                    JobPluginClasspathHelper.thirdPartyJarsFromEnv(envReadonly);

            List<URL> sourceConnectorJars =
                    JobPluginClasspathHelper.connectorJarList(
                            sourceConfigs, PluginType.SOURCE, commonPluginJars);
            List<URL> transformConnectorJars =
                    JobPluginClasspathHelper.connectorJarList(
                            transformConfigs, PluginType.TRANSFORM, commonPluginJars);
            List<URL> sinkConnectorJars =
                    JobPluginClasspathHelper.connectorJarList(
                            sinkConfigs, PluginType.SINK, commonPluginJars);

            List<URL> sourceAndTransformJarUrls =
                    JobPluginClasspathHelper.mergedSourceAndTransformJarUrls(
                            sourceConnectorJars, transformConnectorJars);

            ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader sourceAndTransformClassLoader =
                    JobPluginClasspathHelper.createLocalPluginClassLoader(
                            parentClassLoader, sourceAndTransformJarUrls);
            ClassLoader sinkClassLoader =
                    JobPluginClasspathHelper.createLocalPluginClassLoader(
                            parentClassLoader, sinkConnectorJars);

            try {
                // Set source+transform classloader on the thread so FactoryUtil SPI scanning
                // resolves the same factories as the real runtime parse path.
                Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
                ConfigParserUtil.checkGraph(sourceConfigs, transformConfigs, sinkConfigs);
                validatePluginConfigs(
                        sourceConfigs, TableSourceFactory.class, sourceAndTransformClassLoader);
                validatePluginConfigs(
                        transformConfigs,
                        TableTransformFactory.class,
                        sourceAndTransformClassLoader);

                // Switch to sink classloader before validating sink plugins.
                Thread.currentThread().setContextClassLoader(sinkClassLoader);
                validatePluginConfigs(sinkConfigs, TableSinkFactory.class, sinkClassLoader);
            } finally {
                Thread.currentThread().setContextClassLoader(parentClassLoader);
            }

        } catch (Exception e) {
            throw new ConfigCheckException("Static analysis failed: " + e.getMessage(), e);
        }
    }

    private void validateOptionTypes(ReadonlyConfig config, OptionRule rule) {
        if (rule == null) {
            return;
        }

        for (Option<?> option : rule.getOptionalOptions()) {
            config.getOptional(option);
        }

        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            for (Option<?> option : requiredOption.getOptions()) {
                config.getOptional(option);
            }
        }
    }

    /**
     * Validates each plugin's option rules and, for the {@code Sql} transform, SQL syntax.
     *
     * <p>The caller is responsible for setting {@code pluginClassLoader} as the thread context
     * classloader before calling this method so that {@link FactoryUtil#discoverFactory} resolves
     * the correct factories.
     *
     * @param pluginConfigs raw plugin {@link Config} blocks to validate
     * @param factoryClass the factory interface to discover (source / transform / sink)
     * @param pluginClassLoader the classloader with the connector JARs on its search path
     */
    private void validatePluginConfigs(
            List<? extends Config> pluginConfigs,
            Class<? extends Factory> factoryClass,
            ClassLoader pluginClassLoader)
            throws ConfigCheckException {
        for (Config pluginConfig : pluginConfigs) {
            ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
            String pluginName = readonlyConfig.getOptional(PLUGIN_NAME).orElse("");

            if (StringUtils.isBlank(pluginName)) {
                throw new ConfigCheckException("Plugin name is required but missing");
            }

            Factory factory =
                    FactoryUtil.discoverFactory(pluginClassLoader, factoryClass, pluginName);

            ConfigValidator.of(readonlyConfig).validate(factory.optionRule());
            validateOptionTypes(readonlyConfig, factory.optionRule());
            ConfigValidator.validateUnknownKeys(readonlyConfig, factory.optionRule(), pluginName);

            if ("Sql".equalsIgnoreCase(pluginName)
                    && TableTransformFactory.class.isAssignableFrom(factoryClass)) {
                validateSqlTransformSyntax(pluginConfig, pluginClassLoader);
            }
        }
    }

    private void validateSqlTransformSyntax(Config pluginConfig, ClassLoader pluginClassLoader)
            throws ConfigCheckException {
        if (!pluginConfig.hasPath("query")) {
            return;
        }

        String query = pluginConfig.getString("query");
        if (StringUtils.isBlank(query)) {
            return;
        }

        try {
            Class<?> parserUtilClass =
                    Class.forName(
                            "net.sf.jsqlparser.parser.CCJSqlParserUtil", true, pluginClassLoader);
            Method parseMethod = parserUtilClass.getMethod("parse", String.class);
            parseMethod.invoke(null, query);
            log.debug("Successfully validated SQL transform syntax statically.");
        } catch (InvocationTargetException e) {
            throw new ConfigCheckException(
                    "SQL Syntax Error in Sql Transform: " + e.getTargetException().getMessage(),
                    e.getTargetException());
        } catch (Exception e) {
            log.warn("Could not dynamically load JSqlParser for strict SQL syntax validation", e);
        }
    }
}
