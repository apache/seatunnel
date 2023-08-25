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

package org.apache.searunnel.connectors.cdc.db2.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;

import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfig;
import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfigFactory;
import org.apache.searunnel.connectors.cdc.db2.source.Db2Dialect;
import org.apache.searunnel.connectors.cdc.db2.utils.Db2Utils;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.SourceInfo;
import io.debezium.jdbc.JdbcConnection;

import java.util.Map;

public class LsnOffsetFactory extends OffsetFactory {

    private final Db2SourceConfig sourceConfig;

    private final Db2Dialect dialect;

    public LsnOffsetFactory(Db2SourceConfigFactory configFactory, Db2Dialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        return LsnOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset neverStop() {
        return LsnOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return Db2Utils.currentLsn((Db2Connection) jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return LsnOffset.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY));
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
