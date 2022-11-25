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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceCommonOptions;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Use SPI to create {@link TableSourceFactory}, {@link TableSinkFactory} and {@link CatalogFactory}.
 */
public final class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static <T, SplitT extends SourceSplit, StateT extends Serializable> List<SeaTunnelSource<T, SplitT, StateT>> createAndPrepareSource(
        List<CatalogTable> multipleTables,
        Map<String, String> options,
        ClassLoader classLoader,
        String factoryIdentifier) {

        try {
            final TableSourceFactory factory = discoverFactory(classLoader, TableSourceFactory.class, factoryIdentifier);
            List<SeaTunnelSource<T, SplitT, StateT>> sources = new ArrayList<>(multipleTables.size());
            if (factory instanceof SupportMultipleTable) {
                TableFactoryContext context = new TableFactoryContext(multipleTables, options, classLoader);
                SupportMultipleTable multipleTableSourceFactory = (SupportMultipleTable) factory;
                // TODO: create all source
                SupportMultipleTable.Result result = multipleTableSourceFactory.applyTables(context);
                TableSource<T, SplitT, StateT> multipleTableSource = factory.createSource(
                    new TableFactoryContext(result.getAcceptedTables(), options, classLoader));
                // TODO: handle reading metadata
                SeaTunnelSource<T, SplitT, StateT> source = multipleTableSource.createSource();
                sources.add(source);
            }
            return sources;
        } catch (Throwable t) {
            throw new FactoryException(
                String.format(
                    "Unable to create a source for identifier '%s'.", factoryIdentifier),
                t);
        }
    }

    public static <IN, StateT, CommitInfoT, AggregatedCommitInfoT> SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createAndPrepareSink(
        ClassLoader classLoader, String factoryIdentifier) {
        // todo: do we need to set table?
        TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT> factory = discoverFactory(classLoader, TableSinkFactory.class, factoryIdentifier);
        return factory.createSink(null).createSink();
    }

    public static Catalog createCatalog(String catalogName,
                                        Map<String, String> options,
                                        ClassLoader classLoader,
                                        String factoryIdentifier) {
        CatalogFactory catalogFactory = discoverFactory(classLoader, CatalogFactory.class, factoryIdentifier);
        return catalogFactory.createCatalog(catalogName, options);
    }

    public static <T extends Factory> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<T> foundFactories = discoverFactories(classLoader, factoryClass);

        if (foundFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<T> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::factoryIdentifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        if (matchingFactories.size() > 1) {
            throw new FactoryException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories.get(0);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Factory> List<T> discoverFactories(ClassLoader classLoader, Class<T> factoryClass) {
        return discoverFactories(classLoader).stream()
                .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .map(f -> (T) f)
                .collect(Collectors.toList());
    }

    public static List<Factory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<Factory> result = new LinkedList<>();
            ServiceLoader.load(Factory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for factories.", e);
            throw new FactoryException("Could not load service provider for factories.", e);
        }
    }

    /**
     * This method is called by SeaTunnel Web to get the full option rule of a source.
     * @return
     */
    public static OptionRule sourceFullOptionRule(@NonNull Factory factory) {
        OptionRule sourceOptionRule = factory.optionRule();
        if (sourceOptionRule == null) {
            throw new FactoryException("sourceOptionRule can not be null");
        }

        OptionRule sourceCommonOptionRule =
            OptionRule.builder().optional(SourceCommonOptions.PARALLELISM).build();
        sourceOptionRule.getOptionalOptions().addAll(sourceCommonOptionRule.getOptionalOptions());
        return sourceOptionRule;
    }
}
