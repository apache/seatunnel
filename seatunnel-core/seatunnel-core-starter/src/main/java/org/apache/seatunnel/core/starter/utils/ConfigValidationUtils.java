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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.EnvOptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.ensureJobModeMatch;

/** Utility methods for validating SeaTunnel job configuration without executing a job. */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ConfigValidationUtils {

    private static final BiConsumer<ClassLoader, List<URL>> ADD_URL_TO_CLASSLOADER =
            (classLoader, urls) -> {
                if (classLoader instanceof URLClassLoader) {
                    urls.forEach(url -> ReflectionUtils.invoke(classLoader, "addURL", url));
                } else {
                    try {
                        Optional<Method> method =
                                ReflectionUtils.getDeclaredMethod(
                                        URLClassLoader.class, "addURL", URL.class);
                        if (!method.isPresent()) {
                            throw new IllegalStateException(
                                    "Unable to find addURL method from URLClassLoader");
                        }
                        method.get().setAccessible(true);
                        for (URL url : urls) {
                            method.get().invoke(classLoader, url);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Unsupported classloader: " + classLoader.getClass().getName(), e);
                    }
                }
            };

    private ConfigValidationUtils() {}

    public static void validate(Config config) {
        validate(config, CheckResult.success());
    }

    public static void validate(Config config, CheckResult checkResult) {
        if (!checkResult.isSuccess()) {
            throw new ConfigCheckException(checkResult.getMsg());
        }

        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader validationClassLoader = prepareClassLoader(config, parentClassLoader);
        Thread.currentThread().setContextClassLoader(validationClassLoader);
        try {
            validateEnvironmentConfig(config);
            JobContext jobContext = new JobContext();
            jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));
            jobContext.setEnableCheckpoint(RuntimeEnvironment.getEnableCheckpoint(config));

            List<TableInfo> sourceTables =
                    validateSources(
                            TypesafeConfigUtils.getConfigList(
                                    config, Constants.SOURCE, Collections.emptyList()),
                            jobContext);
            if (sourceTables.isEmpty()) {
                throw new ConfigCheckException("At least one source plugin must be configured.");
            }

            List<TableInfo> outputTables =
                    validateTransforms(
                            sourceTables,
                            TypesafeConfigUtils.getConfigList(
                                    config, Constants.TRANSFORM, Collections.emptyList()),
                            jobContext);

            validateSinks(
                    outputTables,
                    TypesafeConfigUtils.getConfigList(
                            config, Constants.SINK, Collections.emptyList()),
                    jobContext);
        } catch (ConfigCheckException e) {
            throw e;
        } catch (Exception e) {
            Throwable rootException = ExceptionUtils.getRootException(e);
            String message = rootException.getMessage();
            throw new ConfigCheckException(
                    message == null || message.isEmpty() ? e.getMessage() : message, e);
        } finally {
            Thread.currentThread().setContextClassLoader(parentClassLoader);
        }
    }

    private static ClassLoader prepareClassLoader(Config config, ClassLoader parentClassLoader) {
        List<URL> additionalJars = new ArrayList<>();
        if (config.hasPath(Constants.ENV)) {
            Config envConfig = config.getConfig(Constants.ENV);
            if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
                additionalJars.addAll(
                        Common.getThirdPartyJars(envConfig.getString(EnvCommonOptions.JARS.key()))
                                .stream()
                                .map(Path::toUri)
                                .map(ConfigValidationUtils::toUrl)
                                .collect(Collectors.toList()));
            }
        }
        additionalJars.addAll(
                Common.getPluginsJarDependenciesWithoutConnectorDependency().stream()
                        .map(Path::toUri)
                        .map(ConfigValidationUtils::toUrl)
                        .collect(Collectors.toList()));
        additionalJars.addAll(
                Common.getLibJars().stream()
                        .map(Path::toUri)
                        .map(ConfigValidationUtils::toUrl)
                        .collect(Collectors.toList()));
        additionalJars = additionalJars.stream().distinct().collect(Collectors.toList());
        if (additionalJars.isEmpty()) {
            return parentClassLoader;
        }

        try {
            ADD_URL_TO_CLASSLOADER.accept(parentClassLoader, additionalJars);
            return parentClassLoader;
        } catch (RuntimeException e) {
            return new URLClassLoader(additionalJars.toArray(new URL[0]), parentClassLoader);
        }
    }

    private static URL toUrl(java.net.URI uri) {
        try {
            return uri.toURL();
        } catch (Exception e) {
            throw new ConfigCheckException("Invalid plugin dependency path: " + uri, e);
        }
    }

    private static void validateEnvironmentConfig(Config config) {
        if (!config.hasPath(Constants.ENV)) {
            return;
        }
        ReadonlyConfig envConfig = ReadonlyConfig.fromConfig(config.getConfig(Constants.ENV));
        OptionRule optionRule = new EnvOptionRule().optionRule();
        validateOptionTypes(envConfig, optionRule);
        org.apache.seatunnel.api.configuration.util.ConfigValidator.of(envConfig)
                .validate(optionRule);
    }

    private static void validateOptionTypes(ReadonlyConfig config, OptionRule rule) {
        for (Option<?> option : rule.getOptionalOptions()) {
            config.getOptional(option);
        }
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            for (Option<?> option : requiredOption.getOptions()) {
                config.getOptional(option);
            }
        }
    }

    private static List<TableInfo> validateSources(
            List<? extends Config> sourceConfigs, JobContext jobContext) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(
                        org.apache.seatunnel.api.table.factory.TableSourceFactory.class,
                        ADD_URL_TO_CLASSLOADER);
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery(ADD_URL_TO_CLASSLOADER);
        Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource =
                sourcePluginDiscovery::createPluginInstance;

        List<TableInfo> sourceTables = new ArrayList<>();
        for (Config sourceConfig : sourceConfigs) {
            PluginIdentifier pluginIdentifier =
                    getPluginIdentifier(sourceConfig, PluginType.SOURCE);
            TableSourceFactory sourceFactory =
                    (TableSourceFactory)
                            factoryDiscovery
                                    .createOptionalPluginInstance(pluginIdentifier)
                                    .orElse(null);
            if (sourceFactory != null) {
                org.apache.seatunnel.api.configuration.util.ConfigValidator.validateUnknownKeys(
                        ReadonlyConfig.fromConfig(sourceConfig),
                        sourceFactory.optionRule(),
                        pluginIdentifier.getPluginName());
            }
            ClassLoader sourceClassLoader = getFactoryClassLoader(sourceFactory, classLoader);
            Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> source =
                    FactoryUtil.createAndPrepareSource(
                            ReadonlyConfig.fromConfig(sourceConfig),
                            sourceClassLoader,
                            pluginIdentifier.getPluginName(),
                            fallbackCreateSource,
                            sourceFactory,
                            null);

            source._1().setJobContext(jobContext);
            ensureJobModeMatch(jobContext, source._1());
            sourceTables.add(
                    new TableInfo(
                            source._2(),
                            ReadonlyConfig.fromConfig(sourceConfig).get(PLUGIN_OUTPUT)));
        }
        return sourceTables;
    }

    private static List<TableInfo> validateTransforms(
            List<TableInfo> upstreamTables,
            List<? extends Config> transformConfigs,
            JobContext jobContext) {
        if (transformConfigs.isEmpty()) {
            return upstreamTables;
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableTransformFactory.class, ADD_URL_TO_CLASSLOADER);
        TableInfo defaultInput = upstreamTables.get(0);
        Map<String, TableInfo> outputTables =
                upstreamTables.stream()
                        .collect(
                                Collectors.toMap(
                                        TableInfo::getTableName,
                                        Function.identity(),
                                        (left, right) -> right,
                                        LinkedHashMap::new));

        for (Config transformConfig : transformConfigs) {
            PluginIdentifier pluginIdentifier =
                    getPluginIdentifier(transformConfig, PluginType.TRANSFORM);

            TableInfo inputTable =
                    resolveInputTable(
                                    transformConfig,
                                    new ArrayList<>(outputTables.values()),
                                    "Multiple input tables are not supported in the current version")
                            .orElse(defaultInput);

            TableTransformFactory factory =
                    (TableTransformFactory) factoryDiscovery.createPluginInstance(pluginIdentifier);
            org.apache.seatunnel.api.configuration.util.ConfigValidator.validateUnknownKeys(
                    ReadonlyConfig.fromConfig(transformConfig),
                    factory.optionRule(),
                    pluginIdentifier.getPluginName());
            ClassLoader transformClassLoader = getFactoryClassLoader(factory, classLoader);
            TableTransformFactoryContext context =
                    new TableTransformFactoryContext(
                            inputTable.getCatalogTables(),
                            ReadonlyConfig.fromConfig(transformConfig),
                            transformClassLoader);
            org.apache.seatunnel.api.configuration.util.ConfigValidator.of(context.getOptions())
                    .validate(factory.optionRule());
            SeaTunnelTransform<?> transform = factory.createTransform(context).createTransform();
            transform.setJobContext(jobContext);

            String pluginOutputIdentifier =
                    ReadonlyConfig.fromConfig(transformConfig).get(PLUGIN_OUTPUT);
            outputTables.put(
                    pluginOutputIdentifier,
                    new TableInfo(transform.getProducedCatalogTables(), pluginOutputIdentifier));
        }
        return new ArrayList<>(outputTables.values());
    }

    private static void validateSinks(
            List<TableInfo> upstreamTables,
            List<? extends Config> sinkConfigs,
            JobContext jobContext) {
        if (sinkConfigs.isEmpty()) {
            throw new ConfigCheckException("At least one sink plugin must be configured.");
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSinkFactory.class, ADD_URL_TO_CLASSLOADER);
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery =
                new SeaTunnelSinkPluginDiscovery(ADD_URL_TO_CLASSLOADER);
        Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink =
                sinkPluginDiscovery::createPluginInstance;

        TableInfo defaultInput = upstreamTables.get(upstreamTables.size() - 1);
        for (Config sinkConfig : sinkConfigs) {
            PluginIdentifier pluginIdentifier = getPluginIdentifier(sinkConfig, PluginType.SINK);
            TableSinkFactory sinkFactory =
                    (TableSinkFactory)
                            factoryDiscovery
                                    .createOptionalPluginInstance(pluginIdentifier)
                                    .orElse(null);
            if (sinkFactory != null) {
                org.apache.seatunnel.api.configuration.util.ConfigValidator.validateUnknownKeys(
                        ReadonlyConfig.fromConfig(sinkConfig),
                        sinkFactory.optionRule(),
                        pluginIdentifier.getPluginName());
            }
            ClassLoader sinkClassLoader = getFactoryClassLoader(sinkFactory, classLoader);
            TableInfo inputTable =
                    resolveInputTable(
                                    sinkConfig,
                                    upstreamTables,
                                    "Multiple input tables are not supported in the current version")
                            .orElse(defaultInput);

            Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
            for (CatalogTable catalogTable : inputTable.getCatalogTables()) {
                SeaTunnelSink sink =
                        FactoryUtil.createAndPrepareSink(
                                catalogTable,
                                ReadonlyConfig.fromConfig(sinkConfig),
                                sinkClassLoader,
                                pluginIdentifier.getPluginName(),
                                fallbackCreateSink,
                                sinkFactory);
                sink.setJobContext(jobContext);
                sinks.put(catalogTable.getTableId().toTablePath(), sink);
            }

            if (!sinks.isEmpty()
                    && sinks.values().stream().allMatch(SupportMultiTableSink.class::isInstance)) {
                FactoryUtil.createMultiTableSink(
                        sinks, ReadonlyConfig.fromConfig(sinkConfig), sinkClassLoader);
            }
        }
    }

    static ClassLoader getFactoryClassLoader(Factory factory, ClassLoader fallbackClassLoader) {
        return factory == null ? fallbackClassLoader : factory.getClass().getClassLoader();
    }

    private static Optional<TableInfo> resolveInputTable(
            Config pluginConfig, List<TableInfo> upstreamTables, String multiInputMessage) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        if (!readonlyConfig.getOptional(PLUGIN_INPUT).isPresent()) {
            return Optional.empty();
        }

        List<String> pluginInputIdentifiers = readonlyConfig.get(PLUGIN_INPUT);
        if (pluginInputIdentifiers.isEmpty()) {
            throw new ConfigCheckException("plugin_input must not be empty when configured");
        }
        if (pluginInputIdentifiers.size() > 1) {
            throw new ConfigCheckException(multiInputMessage);
        }

        String pluginInputIdentifier = pluginInputIdentifiers.get(0);
        return Optional.of(
                upstreamTables.stream()
                        .filter(info -> pluginInputIdentifier.equals(info.getTableName()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new ConfigCheckException(
                                                String.format(
                                                        "table %s not found",
                                                        pluginInputIdentifier))));
    }

    private static PluginIdentifier getPluginIdentifier(
            Config pluginConfig, PluginType pluginType) {
        return PluginIdentifier.of(
                EngineType.SEATUNNEL.getEngine(),
                pluginType.getType(),
                pluginConfig.getString(PLUGIN_NAME.key()));
    }

    private static final class TableInfo {
        private final List<CatalogTable> catalogTables;
        private final String tableName;

        private TableInfo(List<CatalogTable> catalogTables, String tableName) {
            this.catalogTables = catalogTables;
            this.tableName = tableName;
        }

        public List<CatalogTable> getCatalogTables() {
            return catalogTables;
        }

        public String getTableName() {
            return tableName;
        }
    }
}
