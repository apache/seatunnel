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

/**
 * Opt-in dry-run validation SPI for sink connectors, used by {@code --dry-run connect} (Layer 1).
 *
 * <p>A {@link TableSinkFactory} that can validate its external system without creating sink
 * writers, committers, or save-mode handlers should additionally implement this interface.
 * Factories that do not implement it are reported as {@code SKIPPED} by the connect dry-run instead
 * of being silently treated as validated.
 *
 * <p>This interface is intentionally separate from {@link TableSinkFactory} so that the stable
 * factory contract keeps a single responsibility and existing connectors are not affected.
 */
public interface SupportSinkDryRunValidation {

    /**
     * Validates sink connectivity and schema compatibility for {@code --dry-run connect} without
     * creating sink writers or writing records.
     *
     * <p>The upstream schema is available through {@link
     * TableSinkFactoryContext#getCatalogTable()}. Implementations can use this hook for
     * metadata-level checks such as credentials, permissions, target existence, target
     * createability, and field/type compatibility. It must not execute save-mode logic or any
     * DDL/DML.
     *
     * @param context sink factory context with upstream catalog table and resolved options
     * @throws Exception when connectivity or schema validation fails
     */
    void validateConnectionForDryRun(TableSinkFactoryContext context) throws Exception;
}
