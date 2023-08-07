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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.env.ParsingMode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Used to process every step(source,transform,sink) in the execution pipeline, contained in the
 * {@link TaskExecution}
 *
 * @param <T> Data type of the execution
 * @param <ENV> Runtime environment of engine
 */
@SuppressWarnings("rawtypes")
public interface PluginExecuteProcessor<T, ENV extends RuntimeEnvironment> {
    List<T> execute(List<T> upstreamDataStreams) throws TaskExecuteException;

    void setRuntimeEnvironment(ENV runtimeEnvironment);

    default SeaTunnelSource createSource(
            SeaTunnelFactoryDiscovery factoryDiscovery,
            SeaTunnelSourcePluginDiscovery sourcePluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig,
            JobContext jobContext) {
        // get current thread classloader
        ClassLoader classLoader =
                Thread.currentThread()
                        .getContextClassLoader(); // try to find factory of this plugin

        // get catalog tables from source config
        final List<CatalogTable> catalogTables = new ArrayList<>();
        final List<CatalogTable> tables =
                CatalogTableUtil.getCatalogTables(pluginConfig, classLoader);
        if (!tables.isEmpty()) {
            catalogTables.addAll(tables);
        }

        // try to find table source factory
        final Factory sourceFactory = factoryDiscovery.createPluginInstance(pluginIdentifier);
        final boolean fallback = isFallback(sourceFactory);
        if (fallback || catalogTables.isEmpty()) {
            return fallbackCreate(
                    sourcePluginDiscovery, pluginIdentifier, pluginConfig, jobContext);
        }

        // create source with source factory
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);

        if (readonlyConfig.get(SourceOptions.DAG_PARSING_MODE) == ParsingMode.SHARDING) {
            CatalogTable shardingTable = catalogTables.get(0);
            catalogTables.clear();
            catalogTables.add(shardingTable);
        }

        TableFactoryContext context =
                new TableFactoryContext(catalogTables, readonlyConfig, classLoader);
        ConfigValidator.of(context.getOptions()).validate(sourceFactory.optionRule());
        TableSource tableSource = ((TableSourceFactory) sourceFactory).createSource(context);
        return tableSource.createSource();
    }

    default boolean isFallback(Factory factory) {
        if (Objects.isNull(factory)) {
            return true;
        }
        try {
            ((TableSourceFactory) factory).createSource(null);
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                return true;
            }
            return true;
        }
        return false;
    }

    default SeaTunnelSource fallbackCreate(
            SeaTunnelSourcePluginDiscovery sourcePluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig,
            JobContext jobContext) {
        SeaTunnelSource source = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        source.setJobContext(jobContext);
        if (jobContext.getJobMode() == JobMode.BATCH
                && source.getBoundedness()
                        == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
            throw new UnsupportedOperationException(
                    String.format(
                            "'%s' source don't support off-line job.", source.getPluginName()));
        }
        return source;
    }
}
