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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Dialect for OceanBase in Oracle compatible mode.
 *
 * <p>The dialect name stays {@code Oracle} (inherited from {@link OracleDialect}) because
 * Oracle-specific execution paths are keyed on the dialect name and OceanBase Oracle mode is Oracle
 * compatible. This class only specializes source connection behavior.
 */
@Slf4j
public class OceanBaseOracleDialect extends OracleDialect {

    /** OceanBase JDBC driver switch that enables server-side prepared statement streaming. */
    private static final String USE_SERVER_PREPARED_STATEMENTS = "useServerPrepStmts";

    /**
     * OceanBase Oracle mode can buffer a large sampled result set in memory, so split planning must
     * use bounded chunk queries instead of sampling.
     */
    @Override
    public boolean supportsSamplingSharding() {
        return false;
    }

    /**
     * Forces server-side prepared statements for source reads so OceanBase Oracle mode streams rows
     * by fetch size instead of loading the complete result set into the JVM.
     */
    @Override
    public void configureSourceConnection(String url, Map<String, String> info) {
        connectionUrlParse(url, info, defaultParameter());
        String configuredValue = info.put(USE_SERVER_PREPARED_STATEMENTS, Boolean.TRUE.toString());
        if (configuredValue != null && !Boolean.parseBoolean(configuredValue)) {
            log.warn(
                    "Override {}={} with true for OceanBase Oracle source to use a server-side cursor and avoid buffering the complete result set",
                    USE_SERVER_PREPARED_STATEMENTS,
                    configuredValue);
        }
    }
}
