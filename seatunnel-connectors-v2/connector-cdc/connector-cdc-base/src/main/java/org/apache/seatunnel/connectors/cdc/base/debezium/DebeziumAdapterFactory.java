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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.apache.seatunnel.api.annotation.Experimental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Factory for loading connector-specific {@link DebeziumAdapter} instances via {@link
 * ServiceLoader}.
 */
@Experimental
public class DebeziumAdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumAdapterFactory.class);

    private DebeziumAdapterFactory() {}

    /**
     * Returns the {@link DebeziumAdapter} whose {@link DebeziumAdapter#supports(String)} method
     * returns {@code true} for the given Debezium connector fully-qualified class name (e.g.,
     * {@code "io.debezium.connector.mysql.MySqlConnector"}).
     *
     * @param connectorClassName the fully-qualified Debezium connector class name, matching the
     *     {@code connector.class} Debezium property
     * @param classLoader the class loader used to discover {@link DebeziumAdapter} registrations
     *     from {@code META-INF/services}
     * @throws IllegalStateException if no matching adapter is found
     */
    public static DebeziumAdapter getAdapter(String connectorClassName, ClassLoader classLoader) {
        LOG.info("Loading DebeziumAdapter for connector class: {}", connectorClassName);
        ServiceLoader<DebeziumAdapter> loader =
                ServiceLoader.load(DebeziumAdapter.class, classLoader);
        List<DebeziumAdapter> matchingAdapters = new ArrayList<>();

        for (DebeziumAdapter adapter : loader) {
            if (adapter.supports(connectorClassName)) {
                matchingAdapters.add(adapter);
            }
        }

        if (matchingAdapters.size() == 1) {
            DebeziumAdapter adapter = matchingAdapters.get(0);
            LOG.info(
                    "Found DebeziumAdapter for {}: {} targeting Debezium {}",
                    connectorClassName,
                    adapter.getClass().getName(),
                    adapter.getDebeziumVersion());
            return adapter;
        }

        if (matchingAdapters.size() > 1) {
            String providerDescriptions =
                    matchingAdapters.stream()
                            .map(
                                    adapter ->
                                            adapter.getClass().getName()
                                                    + " (Debezium "
                                                    + adapter.getDebeziumVersion()
                                                    + ")")
                            .collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "Multiple DebeziumAdapters found for connector class: "
                            + connectorClassName
                            + ". Expected exactly one matching provider, but found: "
                            + providerDescriptions);
        }

        throw new IllegalStateException(
                "No DebeziumAdapter found for connector class: "
                        + connectorClassName
                        + ". Ensure a META-INF/services/"
                        + DebeziumAdapter.class.getName()
                        + " registration is present in the connector module.");
    }
}
