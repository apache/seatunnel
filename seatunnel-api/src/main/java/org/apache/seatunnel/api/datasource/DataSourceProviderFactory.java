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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Utility class for discovering and loading {@link DataSourceProvider} implementations via Java
 * SPI.
 *
 * <p>This class provides methods to:
 *
 * <ul>
 *   <li>Find a specific provider by kind
 *   <li>Handle provider loading errors gracefully
 * </ul>
 */
@Slf4j
public final class DataSourceProviderFactory {

    /**
     * Finds a {@link DataSourceProvider} by its kind identifier.
     *
     * @param kind the kind identifier of the provider to find
     * @return the provider
     * @throws DataSourceProviderException if provider is not found or multiple providers with the
     *     same kind are found
     */
    public static DataSourceProvider getProvider(String kind) {
        List<DataSourceProvider> providers = loadProviders();

        DataSourceProvider matchedProvider = null;
        List<String> matchedKinds = new ArrayList<>();

        for (DataSourceProvider provider : providers) {
            if (provider.kind().equalsIgnoreCase(kind)) {
                if (matchedProvider != null) {
                    log.error(
                            "Multiple DataSourceProvider implementations found for kind '{}': {}",
                            kind,
                            matchedKinds);
                    throw new DataSourceProviderException(
                            String.format(
                                    "Multiple DataSourceProvider implementations found for kind '%s'.\n\n"
                                            + "Ambiguous provider classes are:\n\n%s",
                                    kind, String.join("\n", matchedKinds)));
                }
                matchedProvider = provider;
                matchedKinds.add(provider.getClass().getName());
            }
        }

        if (matchedProvider == null) {
            List<String> availableKinds = new ArrayList<>();
            for (DataSourceProvider provider : providers) {
                availableKinds.add(provider.kind());
            }
            log.debug("No DataSourceProvider found for kind: {}", kind);
            throw new DataSourceProviderException(
                    String.format(
                            "No DataSourceProvider found for kind '%s'.\n\n"
                                    + "Available provider kinds are:\n\n%s",
                            kind, String.join("\n", availableKinds)));
        }

        return matchedProvider;
    }

    /**
     * Clears the provider cache.
     *
     * <p>This method is primarily intended for testing purposes. Currently, this method does
     * nothing as providers are loaded on-demand via SPI without caching.
     */
    public static void clearCache() {
        // No-op for testing compatibility
    }

    /**
     * Loads all providers via ServiceLoader.
     *
     * @return list of all discovered providers
     */
    private static List<DataSourceProvider> loadProviders() {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            List<DataSourceProvider> providers = new ArrayList<>();
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
                                .reduce((a, b) -> a + ", " + b)
                                .orElse(""));
            }

            return providers;
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for DataSourceProvider.", e);
            throw new DataSourceProviderException(
                    "Could not load service provider for DataSourceProvider.", e);
        }
    }
}
