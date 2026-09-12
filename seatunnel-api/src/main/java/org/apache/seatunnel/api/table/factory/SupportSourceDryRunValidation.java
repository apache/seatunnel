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

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.util.List;

/**
 * Opt-in dry-run validation SPI for source connectors, used by {@code --dry-run connect} (Layer 1).
 *
 * <p>A {@link TableSourceFactory} that can validate its external system without creating source
 * readers or reading records should additionally implement this interface. Factories that do not
 * implement it are reported as {@code SKIPPED} by the connect dry-run instead of being silently
 * treated as validated.
 *
 * <p>This interface is intentionally separate from {@link TableSourceFactory} so that the stable
 * factory contract keeps a single responsibility and existing connectors are not affected.
 */
public interface SupportSourceDryRunValidation {

    /**
     * Infers source schemas for {@code --dry-run connect} without creating source readers or
     * reading records.
     *
     * <p>Implementations must return the real schemas that downstream transforms and sinks will see
     * at runtime (for example by querying connector metadata), not a synthetic placeholder.
     *
     * @param context source factory context
     * @return source catalog tables visible to downstream transforms and sinks
     * @throws Exception when connector metadata cannot be read
     */
    List<CatalogTable> inferSchemaForDryRun(TableSourceFactoryContext context) throws Exception;

    /**
     * Validates source connectivity for {@code --dry-run connect} after schema inference.
     *
     * <p>This hook must not create readers or read records. It is intended for metadata-level
     * checks such as credentials, permissions, and source table/topic/path existence.
     *
     * @param context source factory context
     * @param catalogTables schemas inferred for this source
     * @throws Exception when connectivity validation fails
     */
    void validateConnectionForDryRun(
            TableSourceFactoryContext context, List<CatalogTable> catalogTables) throws Exception;
}
