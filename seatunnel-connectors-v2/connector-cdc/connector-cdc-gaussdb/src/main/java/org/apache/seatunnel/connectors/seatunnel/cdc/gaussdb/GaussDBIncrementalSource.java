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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresIncrementalSource;

import java.util.List;

/** Incremental CDC source for GaussDB compatible logical replication. */
public class GaussDBIncrementalSource<T> extends PostgresIncrementalSource<T> {

    /** Source factory identifier exposed in SeaTunnel job configuration. */
    static final String IDENTIFIER = "GaussDB-CDC";

    /**
     * Creates a GaussDB CDC source backed by the PostgreSQL CDC runtime.
     *
     * @param options source options parsed from the job configuration
     * @param catalogTables catalog tables resolved for the captured GaussDB tables
     */
    public GaussDBIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    /**
     * Returns the user-facing connector name for metrics and diagnostics.
     *
     * @return GaussDB CDC connector identifier
     */
    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }
}
