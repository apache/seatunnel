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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSource;

import java.util.List;

/**
 * OceanBase CDC source for the first delivery path that targets OceanBase MySQL compatible mode.
 *
 * <p>The connector intentionally reuses the MySQL CDC runtime because OceanBase Binlog Service
 * exposes a MySQL-compatible change-log interface, which keeps snapshot, incremental, checkpoint,
 * and restore semantics aligned with the existing MySQL connector.
 *
 * @param <T> emitted record type
 */
public class OceanBaseIncrementalSource<T> extends MySqlIncrementalSource<T> {

    /** Stable plugin identifier used by SeaTunnel config and plugin discovery. */
    static final String IDENTIFIER = "OceanBase-CDC";

    /**
     * Create an OceanBase CDC source backed by the MySQL CDC runtime.
     *
     * @param options connector options
     * @param catalogTables tables discovered or restored for the source
     */
    public OceanBaseIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    /**
     * Expose the OceanBase-specific plugin name while reusing MySQL-compatible internals.
     *
     * @return OceanBase CDC plugin name
     */
    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }
}
