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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config.OracleSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.OracleDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils.OracleUtils;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;

import java.util.Map;

public class RedoLogOffsetFactory extends OffsetFactory {

    private final OracleSourceConfig sourceConfig;

    private final OracleDialect dialect;

    public RedoLogOffsetFactory(OracleSourceConfigFactory configFactory, OracleDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        return RedoLogOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset neverStop() {
        return RedoLogOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return OracleUtils.currentScn((OracleConnection) jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new RedoLogOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset timestamp(long timestamp) {
        throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
    }
}
