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

import org.apache.seatunnel.api.metalake.TableSchemaDiscoverer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;

import java.io.Serializable;
import java.util.List;

/**
 * This is an SPI interface, used to create {@link TableSource}. Each plugin need to have it own
 * implementation.
 */
public interface TableSourceFactory extends Factory {

    /**
     * We will never use this method now. So gave a default implement and return null.
     *
     * @param context TableFactoryContext
     */
    default <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        throw new UnsupportedOperationException(
                "The Factory has not been implemented and the deprecated Plugin will be used.");
    }

    /**
     * We can get the catalogTable list in the source configuration through this method
     *
     * @param context TableFactoryContext
     */
    default List<CatalogTable> discoverTableSchemas(TableSourceFactoryContext context) {
        try (TableSchemaDiscoverer metaLakeSchemaDiscoverer =
                new TableSchemaDiscoverer(context, factoryIdentifier())) {
            return metaLakeSchemaDiscoverer.discoverTableSchemas();
        }
    }

    /**
     * Infers source schemas for {@code --dry-run=connect} without creating source readers or
     * reading records.
     *
     * <p>The default implementation delegates to {@link #discoverTableSchemas}. Connectors can
     * override this method when Layer 1 schema inference needs connector-specific metadata access.
     *
     * @param context source factory context
     * @return source catalog tables visible to downstream transforms and sinks
     */
    default List<CatalogTable> inferSchemaForDryRun(TableSourceFactoryContext context) {
        return discoverTableSchemas(context);
    }

    /**
     * Validates source connectivity for {@code --dry-run=connect} after schema inference.
     *
     * <p>This hook must not create readers or read records. It is intended for metadata-level
     * checks such as credentials, permissions, and source table/topic/path existence.
     *
     * @param context source factory context
     * @param catalogTables schemas inferred for this source
     * @throws Exception when connectivity validation fails
     */
    default void validateConnectionForDryRun(
            TableSourceFactoryContext context, List<CatalogTable> catalogTables) throws Exception {}

    /**
     * TODO: Implement SupportParallelism in the TableSourceFactory instead of the SeaTunnelSource,
     * Then deprecated the method
     */
    Class<? extends SeaTunnelSource> getSourceClass();
}
