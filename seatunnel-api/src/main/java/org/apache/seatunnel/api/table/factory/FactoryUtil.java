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
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.env.ParsingMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkFactory;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceOptions;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;

/**
 * Use SPI to create {@link TableSourceFactory}, {@link TableSinkFactory} and {@link
 * CatalogFactory}.
 */
@Slf4j
public final class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static final String DEFAULT_ID = "default-identifier";

    public static <T, SplitT extends SourceSplit, StateT extends Serializable>
            Tuple2<SeaTunnelSource<T, SplitT, StateT>, List<CatalogTable>> createAndPrepareSource(
                    ReadonlyConfig options,
                    ClassLoader classLoader,
                    String factoryIdentifier,
                    Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource,
                    TableSourceFactory factory) {
        return restoreAndPrepareSource(
                options, classLoader, factoryIdentifier, null, fallbackCreateSource, factory);
    }

    public static <T, SplitT extends SourceSplit, StateT extends Serializable>
            Tuple2<SeaTunnelSource<T, SplitT, StateT>, List<CatalogTable>> restoreAndPrepareSource(
                    ReadonlyConfig options,
                    ClassLoader classLoader,
                    String factoryIdentifier,
                    ChangeStreamTableSourceCheckpoint checkpoint,
                    Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource,
                    TableSourceFactory factory) {

        try {

            SeaTunnelSource<T, SplitT, StateT> source;
            final String factoryId = options.get(PLUGIN_NAME);

            boolean fallback =
                    isFallback(
                            classLoader,
                            TableSourceFactory.class,
                            factoryId,
                            (sourceFactory) -> sourceFactory.createSource(null));

            if (fallback) {
                source =
                        fallbackCreateSource.apply(
                                PluginIdentifier.of("seatunnel", "source", factoryId));
                source.prepare(options.toConfig());

            } else {
                if (factory == null) {
                    factory =
                            discoverFactory(
                                    classLoader, TableSourceFactory.class, factoryIdentifier);
                }

                if (factory instanceof ChangeStreamTableSourceFactory && checkpoint != null) {
                    ChangeStreamTableSourceFactory changeStreamTableSourceFactory =
                            (ChangeStreamTableSourceFactory) factory;
                    ChangeStreamTableSourceState<Serializable, SourceSplit> state =
                            changeStreamTableSourceFactory.deserializeTableSourceState(checkpoint);
                    source =
                            restoreAndPrepareSource(
                                    changeStreamTableSourceFactory, options, classLoader, state);
                } else {
                    source = createAndPrepareSource(factory, options, classLoader);
                }
            }
            List<CatalogTable> catalogTables;
            try {
                catalogTables = source.getProducedCatalogTables();
            } catch (UnsupportedOperationException e) {
                // TODO remove it when all connector use `getProducedCatalogTables`
                SeaTunnelDataType<T> seaTunnelDataType = source.getProducedType();
                final String tableId =
                        options.getOptional(CommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);
                catalogTables =
                        CatalogTableUtil.convertDataTypeToCatalogTables(seaTunnelDataType, tableId);
            }
            LOG.info(
                    "get the CatalogTable from source {}: {}",
                    source.getPluginName(),
                    catalogTables.stream()
                            .map(CatalogTable::getTableId)
                            .map(TableIdentifier::toString)
                            .collect(Collectors.joining(",")));
            if (options.get(SourceOptions.DAG_PARSING_MODE) == ParsingMode.SHARDING) {
                CatalogTable catalogTable = catalogTables.get(0);
                catalogTables.clear();
                catalogTables.add(catalogTable);
            }
            return new Tuple2<>(source, catalogTables);

        } catch (Throwable t) {
            throw new FactoryException(
                    String.format(
                            "Unable to create a source for identifier '%s'.", factoryIdentifier),
                    t);
        }
    }

    private static <T, SplitT extends SourceSplit, StateT extends Serializable>
            SeaTunnelSource<T, SplitT, StateT> createAndPrepareSource(
                    TableSourceFactory factory, ReadonlyConfig options, ClassLoader classLoader) {
        TableSourceFactoryContext context = new TableSourceFactoryContext(options, classLoader);
        ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
        TableSource<T, SplitT, StateT> tableSource = factory.createSource(context);
        return tableSource.createSource();
    }

    private static <T, SplitT extends SourceSplit, StateT extends Serializable>
            SeaTunnelSource<T, SplitT, StateT> restoreAndPrepareSource(
                    ChangeStreamTableSourceFactory factory,
                    ReadonlyConfig options,
                    ClassLoader classLoader,
                    ChangeStreamTableSourceState state) {
        TableSourceFactoryContext context = new TableSourceFactoryContext(options, classLoader);
        ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
        LOG.info("Restore create source from checkpoint state: {}", state);
        TableSource<T, SplitT, StateT> tableSource = factory.restoreSource(context, state);
        return tableSource.createSource();
    }

    public static <IN, StateT, CommitInfoT, AggregatedCommitInfoT>
            SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createAndPrepareSink(
                    CatalogTable catalogTable,
                    ReadonlyConfig config,
                    ClassLoader classLoader,
                    String factoryIdentifier,
                    Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink,
                    TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
                            tableSinkFactory) {
        try {
            final String factoryId = config.get(PLUGIN_NAME);

            boolean fallback =
                    isFallback(
                            classLoader,
                            TableSinkFactory.class,
                            factoryId,
                            (sinkFactory) -> sinkFactory.createSink(null));

            if (fallback) {
                SeaTunnelSink sink =
                        fallbackCreateSink.apply(
                                PluginIdentifier.of("seatunnel", "sink", factoryId));
                sink.prepare(config.toConfig());
                sink.setTypeInfo(catalogTable.getSeaTunnelRowType());

                return sink;
            }

            if (tableSinkFactory == null) {
                tableSinkFactory =
                        discoverFactory(classLoader, TableSinkFactory.class, factoryIdentifier);
            }

            TableSinkFactoryContext context =
                    TableSinkFactoryContext.replacePlaceholderAndCreate(
                            catalogTable,
                            config,
                            classLoader,
                            tableSinkFactory.excludeTablePlaceholderReplaceKeys());
            ConfigValidator.of(context.getOptions()).validate(tableSinkFactory.optionRule());

            LOG.info(
                    "Create sink '{}' with upstream input catalog-table[database: {}, schema: {}, table: {}]",
                    factoryIdentifier,
                    catalogTable.getTablePath().getDatabaseName(),
                    catalogTable.getTablePath().getSchemaName(),
                    catalogTable.getTablePath().getTableName());
            return tableSinkFactory.createSink(context).createSink();
        } catch (Throwable t) {
            throw new FactoryException(
                    String.format(
                            "Unable to create a sink for identifier '%s'.", factoryIdentifier),
                    t);
        }
    }

    public static <IN, StateT, CommitInfoT, AggregatedCommitInfoT>
            SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createMultiTableSink(
                    Map<TablePath, SeaTunnelSink> sinks,
                    ReadonlyConfig options,
                    ClassLoader classLoader) {
        try {
            TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT> factory =
                    new MultiTableSinkFactory();
            MultiTableFactoryContext context =
                    new MultiTableFactoryContext(options, classLoader, sinks);
            ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
            return factory.createSink(context).createSink();
        } catch (Throwable t) {
            throw new FactoryException(
                    "Unable to create a sink for identifier 'MultiTableSink'.", t);
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

    public static SeaTunnelTransform<?> createAndPrepareMultiTableTransform(
            List<CatalogTable> catalogTables,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryIdentifier) {
        final TableTransformFactory factory =
                discoverFactory(classLoader, TableTransformFactory.class, factoryIdentifier);
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(catalogTables, options, classLoader);
        ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
        return factory.createTransform(context).createTransform();
    }

    private static <T extends Factory> boolean isFallback(
            ClassLoader classLoader,
            Class<T> factoryClass,
            String factoryId,
            Consumer<T> virtualCreator) {
        Optional<T> factory = discoverOptionalFactory(classLoader, factoryClass, factoryId);
        if (!factory.isPresent()) {
            return true;
        }
        try {
            virtualCreator.accept(factory.get());
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                return true;
            }
            log.debug(ExceptionUtils.getMessage(e));
        }
        return false;
    }
}
