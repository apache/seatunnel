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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset;

import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;

import io.debezium.jdbc.JdbcConnection;

import java.util.Map;

/** An offset factory class create {@link BinlogOffset} instance. */
public class BinlogOffsetFactory extends OffsetFactory {

    private final MySqlSourceConfig sourceConfig;

    private final JdbcDataSourceDialect dialect;

    public BinlogOffsetFactory(MySqlSourceConfigFactory configFactory,
                               JdbcDataSourceDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return MySqlConnectionUtils.earliestBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset neverStop() {
        return BinlogOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return MySqlConnectionUtils.currentBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new BinlogOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        return new BinlogOffset(filename, position);
    }

    @Override
    public Offset timestamp(long timestamp) {
        throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
    }
}
