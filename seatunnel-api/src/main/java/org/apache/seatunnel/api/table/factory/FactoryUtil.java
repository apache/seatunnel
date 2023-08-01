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

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NonNull;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Use SPI to create {@link TableSourceFactory}, {@link TableSinkFactory} and {@link
 * CatalogFactory}.
 */
public final class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static <T, SplitT extends SourceSplit, StateT extends Serializable>
            List<Tuple2<SeaTunnelSource<T, SplitT, StateT>, List<CatalogTable>>>
                    createAndPrepareSource(
                            List<CatalogTable> multipleTables,
                            ReadonlyConfig options,
                            ClassLoader classLoader,
                            String factoryIdentifier) {

        try {
            final TableSourceFactory factory =
                    discoverFactory(classLoader, TableSourceFactory.class, factoryIdentifier);
            List<Tuple2<SeaTunnelSource<T, SplitT, StateT>, List<CatalogTable>>> sources =
                    new ArrayList<>(multipleTables.size());
            if (factory instanceof SupportMultipleTable) {
                List<CatalogTable> remainingTables = multipleTables;
                while (!remainingTables.isEmpty()) {
                    TableFactoryContext context =
                            new TableFactoryContext(remainingTables, options, classLoader);
                    SupportMultipleTable.Result result =
                            ((SupportMultipleTable) factory).applyTables(context);
                    List<CatalogTable> acceptedTables = result.getAcceptedTables();
                    sources.add(
                            new Tuple2<>(
                                    createAndPrepareSource(
                                            factory, acceptedTables, options, classLoader),
                                    acceptedTables));
                    remainingTables = result.getRemainingTables();
                }
            } else {
                for (CatalogTable catalogTable : multipleTables) {
                    List<CatalogTable> acceptedTables = Collections.singletonList(catalogTable);
                    sources.add(
                            new Tuple2<>(
                                    createAndPrepareSource(
                                            factory, acceptedTables, options, classLoader),
                                    acceptedTables));
                }
            }
            return sources;
        } catch (Throwable t) {
            throw new FactoryException(
                    String.format(
                            "Unable to create a source for identifier '%s'.", factoryIdentifier),
                    t);
        }
    }

    private static <T, SplitT extends SourceSplit, StateT extends Serializable>
            SeaTunnelSource<T, SplitT, StateT> createAndPrepareSource(
                    TableSourceFactory factory,
                    List<CatalogTable> acceptedTables,
                    ReadonlyConfig options,
                    ClassLoader classLoader) {
        TableFactoryContext context = new TableFactoryContext(acceptedTables, options, classLoader);
        ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
        TableSource<T, SplitT, StateT> tableSource = factory.createSource(context);
        validateAndApplyMetadata(acceptedTables, tableSource);
        return tableSource.createSource();
    }

    private static void validateAndApplyMetadata(
            List<CatalogTable> catalogTables, TableSource<?, ?, ?> tableSource) {
        // TODO: handle reading metadata
    }

    public static <IN, StateT, CommitInfoT, AggregatedCommitInfoT>
            SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createAndPrepareSink(
                    CatalogTable catalogTable,
                    ReadonlyConfig options,
                    ClassLoader classLoader,
                    String factoryIdentifier) {
        try {
            TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT> factory =
                    discoverFactory(classLoader, TableSinkFactory.class, factoryIdentifier);
            TableFactoryContext context =
                    new TableFactoryContext(
                            Collections.singletonList(catalogTable), options, classLoader);
            ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
            return factory.createSink(context).createSink();
        } catch (Throwable t) {
            throw new FactoryException(
                    String.format(
                            "Unable to create a sink for identifier '%s'.", factoryIdentifier),
                    t);
        }
    }

    public static Optional<Catalog> createOptionalCatalog(
            String catalogName,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryIdentifier) {
        Optional<CatalogFactory> optionalFactory =
                discoverOptionalFactory(classLoader, CatalogFactory.class, factoryIdentifier);
        return optionalFactory.map(
                catalogFactory -> catalogFactory.createCatalog(catalogName, options));
    }

    public static <T extends Factory> URL getFactoryUrl(T factory) {
        return factory.getClass().getProtectionDomain().getCodeSource().getLocation();
    }

    public static <T extends Factory> Optional<T> discoverOptionalFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<T> foundFactories = discoverFactories(classLoader, factoryClass);
        if (foundFactories.isEmpty()) {
            return Optional.empty();
        }
        final List<T> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equalsIgnoreCase(factoryIdentifier))
                        .collect(Collectors.toList());
        if (matchingFactories.isEmpty()) {
            return Optional.empty();
        }
        checkMultipleMatchingFactories(factoryIdentifier, factoryClass, matchingFactories);
        return Optional.of(matchingFactories.get(0));
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
                        .filter(f -> f.factoryIdentifier().equalsIgnoreCase(factoryIdentifier))
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

        checkMultipleMatchingFactories(factoryIdentifier, factoryClass, matchingFactories);

        return matchingFactories.get(0);
    }

    private static <T extends Factory> void checkMultipleMatchingFactories(
            String factoryIdentifier, Class<T> factoryClass, List<T> matchingFactories) {
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
    }

    @SuppressWarnings("unchecked")
    public static <T extends Factory> List<T> discoverFactories(
            ClassLoader classLoader, Class<T> factoryClass) {
        return discoverFactories(classLoader).stream()
                .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .map(f -> (T) f)
                .collect(Collectors.toList());
    }

    public static List<Factory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<Factory> result = new LinkedList<>();
            ServiceLoader.load(Factory.class, classLoader).iterator().forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for factories.", e);
            throw new FactoryException("Could not load service provider for factories.", e);
        }
    }

    /**
     * This method is called by SeaTunnel Web to get the full option rule of a source.
     *
     * @return Option rule
     */
    public static OptionRule sourceFullOptionRule(@NonNull TableSourceFactory factory) {
        OptionRule sourceOptionRule = factory.optionRule();
        if (sourceOptionRule == null) {
            throw new FactoryException("sourceOptionRule can not be null");
        }

        Class<? extends SeaTunnelSource> sourceClass = factory.getSourceClass();
        if (factory instanceof SupportParallelism
                // TODO: Implement SupportParallelism in the TableSourceFactory instead of the
                // SeaTunnelSource
                || SupportParallelism.class.isAssignableFrom(sourceClass)) {
            OptionRule sourceCommonOptionRule =
                    OptionRule.builder().optional(CommonOptions.PARALLELISM).build();
            sourceOptionRule
                    .getOptionalOptions()
                    .addAll(sourceCommonOptionRule.getOptionalOptions());
        }

        return sourceOptionRule;
    }

    /**
     * This method is called by SeaTunnel Web to get the full option rule of a sink.
     *
     * @return Option rule
     */
    public static OptionRule sinkFullOptionRule(@NonNull TableSinkFactory factory) {
        OptionRule sinkOptionRule = factory.optionRule();
        if (sinkOptionRule == null) {
            throw new FactoryException("sinkOptionRule can not be null");
        }
        return sinkOptionRule;
    }

    public static SeaTunnelTransform<?> createAndPrepareTransform(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryIdentifier) {
        final TableTransformFactory factory =
                discoverFactory(classLoader, TableTransformFactory.class, factoryIdentifier);
        TableFactoryContext context =
                new TableFactoryContext(
                        Collections.singletonList(catalogTable), options, classLoader);
        ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
        return factory.createTransform(context).createTransform();
    }
}
