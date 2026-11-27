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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset;

import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.MariaDbSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbConnectionUtils;

import io.debezium.jdbc.JdbcConnection;

import java.util.Map;

/** An offset factory class to create {@link MariaDbBinlogOffset} instances. */
public class MariaDbBinlogOffsetFactory extends OffsetFactory {

    private final MariaDbSourceConfig sourceConfig;
    private final JdbcDataSourceDialect dialect;

    public MariaDbBinlogOffsetFactory(
            MariaDbSourceConfigFactory configFactory, JdbcDataSourceDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return MariaDbConnectionUtils.earliestBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the MariaDB binlog offset error", e);
        }
    }

    @Override
    public Offset neverStop() {
        return MariaDbBinlogOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return MariaDbConnectionUtils.currentBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the MariaDB binlog offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new MariaDbBinlogOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        return new MariaDbBinlogOffset(filename, position);
    }

    @Override
    public Offset timestamp(long timestamp) {
        return new MariaDbBinlogOffset(timestamp / 1000);
    }
}
