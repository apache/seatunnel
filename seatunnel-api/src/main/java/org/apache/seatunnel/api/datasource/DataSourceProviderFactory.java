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

package org.apache.seatunnel.api.datasource;

import org.apache.seatunnel.api.datasource.exception.DataSourceProviderException;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Utility class for discovering and loading {@link DataSourceProvider} implementations via Java
 * SPI.
 *
 * <p>This class provides methods to:
 *
 * <ul>
 *   <li>Discover all available data source providers
 *   <li>Find a specific provider by kind
 *   <li>Handle provider loading errors gracefully
 * </ul>
 *
 * <p>Discovered providers are cached by kind to avoid repeated ServiceLoader lookups.
 */
@Slf4j
public final class DataSourceProviderFactory {
    /** Cache for all discovered providers by kind. */
    private static final ConcurrentMap<String, DataSourceProvider> PROVIDER_CACHE =
            new ConcurrentHashMap<>();

    /** Cache for provider list. */
    private static volatile List<DataSourceProvider> cachedProviders = null;

    /**
     * Finds a {@link DataSourceProvider} by its kind identifier or throws an exception if not
     * found.
     *
     * @param kind the kind identifier of the provider to find
     * @return the provider
     * @throws DataSourceProviderException if provider is not found or multiple providers with the
     *     same kind are found
     */
    public static DataSourceProvider getProvider(String kind) {
        return findProvider(kind)
                .orElseThrow(
                        () -> {
                            List<String> availableKinds =
                                    discoverProviders().stream()
                                            .map(DataSourceProvider::kind)
                                            .distinct()
                                            .sorted()
                                            .collect(Collectors.toList());

                            return new DataSourceProviderException(
                                    String.format(
                                            "No DataSourceProvider found for kind '%s'.\n\n"
                                                    + "Available provider kinds are:\n\n%s",
                                            kind, String.join("\n", availableKinds)));
                        });
    }

    /**
     * Discovers all available {@link DataSourceProvider} implementations.
     *
     * <p>Results are cached, subsequent calls will return the cached providers.
     *
     * @return list of all discovered providers
     * @throws DataSourceProviderException if SPI loading fails
     */
    private static List<DataSourceProvider> discoverProviders() {
        if (cachedProviders == null) {
            synchronized (DataSourceProviderFactory.class) {
                if (cachedProviders == null) {
                    cachedProviders = loadProviders();
                }
            }
        }
        return cachedProviders;
    }

    /**
     * Finds a {@link DataSourceProvider} by its kind identifier.
     *
     * <p>Results are cached by kind, subsequent calls will return the cached provider.
     *
     * @param kind the kind identifier of the provider to find
     * @return Optional containing the provider if found, empty otherwise
     * @throws DataSourceProviderException if SPI loading fails or multiple providers with the same
     *     kind are found
     */
    private static Optional<DataSourceProvider> findProvider(String kind) {
        DataSourceProvider cached = PROVIDER_CACHE.get(kind);
        if (cached != null) {
            return Optional.of(cached);
        }
        // Not in cache, discover and cache
        List<DataSourceProvider> providers = discoverProviders();
        List<DataSourceProvider> matching =
                providers.stream()
                        .filter(p -> p.kind().equalsIgnoreCase(kind))
                        .collect(Collectors.toList());

        if (matching.isEmpty()) {
            log.debug("No DataSourceProvider found for kind: {}", kind);
            return Optional.empty();
        }

        if (matching.size() > 1) {
            String duplicateProviders =
                    matching.stream()
                            .map(p -> p.getClass().getName())
                            .sorted()
                            .collect(Collectors.joining("\n"));
            log.error(
                    "Multiple DataSourceProvider implementations found for kind '{}':\n{}",
                    kind,
                    duplicateProviders);
            throw new DataSourceProviderException(
                    String.format(
                            "Multiple DataSourceProvider implementations found for kind '%s'.\n\n"
                                    + "Ambiguous provider classes are:\n\n%s",
                            kind, duplicateProviders));
        }

        DataSourceProvider provider = matching.get(0);
        PROVIDER_CACHE.put(kind, provider);
        return Optional.of(provider);
    }

    /**
     * Clears all cached providers.
     *
     * <p>This method is primarily intended for testing purposes.
     */
    public static void clearCache() {
        PROVIDER_CACHE.clear();
        cachedProviders = null;
        log.debug("DataSourceProvider cache cleared");
    }

    /**
     * Loads providers via ServiceLoader.
     *
     * @return list of all discovered providers
     */
    private static List<DataSourceProvider> loadProviders() {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            List<DataSourceProvider> providers = new LinkedList<>();
            ServiceLoader.load(DataSourceProvider.class, classLoader)
                    .iterator()
                    .forEachRemaining(providers::add);

            if (providers.isEmpty()) {
                log.info("No DataSourceProvider implementations found");
            } else {
                log.info(
                        "Loaded {} DataSourceProvider: {}",
                        providers.size(),
                        providers.stream()
                                .map(DataSourceProvider::kind)
                                .collect(Collectors.joining(", ")));
            }

            return providers;
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for DataSourceProvider.", e);
            throw new DataSourceProviderException(
                    "Could not load service provider for DataSourceProvider.", e);
        }
    }
}
