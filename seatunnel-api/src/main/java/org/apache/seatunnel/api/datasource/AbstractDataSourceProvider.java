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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Abstract base class for {@link DataSourceProvider} implementations.
 *
 * <p>This class provides thread-safe caching for data source mappers, using double-checked locking
 * with {@code synchronized} for lazy initialization.
 *
 * <p>Subclasses only need to implement {@link #createDataSourceMappers()} to provide the mapper
 * map, which will be cached after the first call.
 *
 * <h2>Usage Example</h2>
 *
 * <pre>
 * &#64;AutoService(DataSourceProvider.class)
 * public class MyDataSourceProvider extends AbstractDataSourceProvider {
 *
 *     &#64;Override
 *     public String kind() {
 *         return "my-provider";
 *     }
 *
 *     &#64;Override
 *     protected Map&lt;String, DataSourceMapper&gt; createDataSourceMappers() {
 *         Map&lt;String, DataSourceMapper&gt; mappers = new HashMap&lt;&gt;();
 *         mappers.put("Jdbc", new MyJdbcMapper());
 *         mappers.put("Kafka", new MyKafkaMapper());
 *         return mappers;
 *     }
 *
 *     // implement init() and close()
 * }
 * </pre>
 */
public abstract class AbstractDataSourceProvider implements DataSourceProvider {

    private volatile Map<String, DataSourceMapper> cachedMappers;

    @Override
    public final Collection<DataSourceMapper> dataSourceMappers() {
        if (cachedMappers == null) {
            synchronized (this) {
                if (cachedMappers == null) {
                    cachedMappers = createDataSourceMappers();
                    // Make the cached map unmodifiable for thread safety
                    cachedMappers = Collections.unmodifiableMap(cachedMappers);
                }
            }
        }
        return cachedMappers.values();
    }

    /**
     * Gets the data source mapper for a specific connector identifier.
     *
     * <p>This is a convenience method for directly looking up a mapper by connector identifier,
     * avoiding the need to iterate through the collection returned by {@link #dataSourceMappers()}.
     *
     * @param connectorIdentifier the connector identifier (e.g., "Jdbc", "Kafka")
     * @return the matching mapper, or null if not found
     */
    public final DataSourceMapper getMapper(String connectorIdentifier) {
        // Ensure mappers are initialized
        dataSourceMappers();
        return cachedMappers.get(connectorIdentifier);
    }

    /**
     * Creates the map of data source mappers supported by this provider.
     *
     * <p>This method is called once (lazily) when {@link #dataSourceMappers()} is first invoked.
     * Subclasses should implement this method to provide their mappers.
     *
     * <p>The returned map is cached and reused for subsequent calls. The map key should be the
     * connector identifier (e.g., "Jdbc", "Kafka") and the value should be the corresponding
     * mapper.
     *
     * @return map of connector identifier to data source mapper
     */
    protected abstract Map<String, DataSourceMapper> createDataSourceMappers();
}
