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

package org.apache.seatunnel.core.starter.execution;

import com.google.common.collect.Lists;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryException;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;

/** The util used for Spark/Flink to create to SeaTunnelSource etc. */
@SuppressWarnings("rawtypes")
public class PluginUtil {

    protected static final String ENGINE_TYPE = "seatunnel";

    public static SourceTableInfo createSource(
            PluginDiscovery<Factory> factoryDiscovery,
            PluginDiscovery<SeaTunnelSource> sourcePluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig,
            JobContext jobContext) {
        // get current thread classloader
        ClassLoader classLoader =
                Thread.currentThread()
                        .getContextClassLoader(); // try to find factory of this plugin

        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        // try to find table source factory
        final Optional<Factory> sourceFactory =
                factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        final boolean fallback = isFallback(sourceFactory);
        SeaTunnelSource source;
        if (fallback) {
            source = fallbackCreate(sourcePluginDiscovery, pluginIdentifier, pluginConfig);
        } else {
            // create source with source factory
            TableSourceFactoryContext context =
                    new TableSourceFactoryContext(readonlyConfig, classLoader);
            ConfigValidator.of(context.getOptions()).validate(sourceFactory.get().optionRule());
            TableSource tableSource =
                    ((TableSourceFactory) sourceFactory.get()).createSource(context);
            source = tableSource.createSource();
        }
        source.setJobContext(jobContext);
        ensureJobModeMatch(jobContext, source);
        List<CatalogTable> catalogTables;
        try {
            catalogTables = source.getProducedCatalogTables();
        } catch (UnsupportedOperationException e) {
            // TODO remove it when all connector use `getProducedCatalogTables`
            SeaTunnelDataType<?> seaTunnelDataType = source.getProducedType();
            final String tableId =
                    readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME).orElse(DEFAULT_ID);
            catalogTables =
                    CatalogTableUtil.convertDataTypeToCatalogTables(seaTunnelDataType, tableId);
        }
        return new SourceTableInfo(source, catalogTables);
    }

    private static boolean isFallback(Optional<Factory> factory) {
        if (!factory.isPresent()) {
            return true;
        }
        try {
            ((TableSourceFactory) factory.get()).createSource(null);
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                return true;
            }
        }
        return false;
    }

    private static SeaTunnelSource fallbackCreate(
            PluginDiscovery<SeaTunnelSource> sourcePluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        SeaTunnelSource source = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
    }

    public static Optional<? extends Factory> createTransformFactory(
            PluginDiscovery<Factory> factoryDiscovery,
            PluginDiscovery<SeaTunnelTransform> transformPluginDiscovery,
            Config transformConfig,
            List<URL> pluginJars) {
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        ENGINE_TYPE, "transform", transformConfig.getString(PLUGIN_NAME.key()));
        pluginJars.addAll(
                transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
        try {
            return factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        } catch (FactoryException e) {
            return Optional.empty();
        }
    }

    public static Optional<? extends Factory> createSinkFactory(
            PluginDiscovery<Factory> factoryDiscovery,
            PluginDiscovery<SeaTunnelSink> sinkPluginDiscovery,
            Config sinkConfig,
            List<URL> pluginJars) {
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(ENGINE_TYPE, "sink", sinkConfig.getString(PLUGIN_NAME.key()));
        pluginJars.addAll(
                sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
        try {
            return factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        } catch (FactoryException e) {
            return Optional.empty();
        }
    }

    public static void ensureJobModeMatch(JobContext jobContext, SeaTunnelSource source) {
        if (jobContext.getJobMode() == JobMode.BATCH
                && source.getBoundedness()
                        == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
            throw new UnsupportedOperationException(
                    String.format(
                            "'%s' source don't support off-line job.", source.getPluginName()));
        }
    }
}
