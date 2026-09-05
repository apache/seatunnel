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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresIncrementalSource;

import java.util.List;

/**
 * OpenGauss source entry point that reuses the shared PG-base PostgreSQL implementation while
 * preserving the existing Apache OpenGauss runtime behavior.
 */
public class OpengaussIncrementalSource<T> extends PostgresIncrementalSource<T> {

    // Same reason as PgBaseIncrementalSource: this source is Java-serialized into the job DAG,
    // so the UID must not drift when this class is edited later.
    private static final long serialVersionUID = 1L;

    private static final String IDENTIFIER = "Opengauss-CDC";

    public OpengaussIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    /**
     * Resolves startup mode against the OpenGauss option instead of the PostgreSQL one, so this
     * connector keeps the three modes it has always accepted rather than inheriting PostgreSQL's
     * WAL-slot-specific additions. See {@link OpengaussSourceOptions#STARTUP_MODE}.
     */
    @Override
    public Option<StartupMode> getStartupModeOption() {
        return OpengaussSourceOptions.STARTUP_MODE;
    }
}
