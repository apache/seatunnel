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

package org.apache.seatunnel.connectors.cdc.dameng.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfig;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.dameng.source.DamengDialect;

import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.Scn;
import io.debezium.connector.dameng.SourceInfo;

import java.util.Map;

public class LogMinerOffsetFactory extends OffsetFactory {
    private final DamengSourceConfig sourceConfig;
    private final DamengDialect dialect;

    public LogMinerOffsetFactory(DamengSourceConfigFactory configFactory,
                                 DamengDialect dialect) {
        this(configFactory.create(0), dialect);
    }

    public LogMinerOffsetFactory(DamengSourceConfig sourceConfig,
                                 DamengDialect dialect) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        try (DamengConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return new LogMinerOffset(jdbcConnection.earliestLsn());
        } catch (Exception e) {
            throw new RuntimeException("Read the logminer offset error", e);
        }
    }

    @Override
    public Offset neverStop() {
        return LogMinerOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (DamengConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return new LogMinerOffset(jdbcConnection.currentCheckpointLsn());
        } catch (Exception e) {
            throw new RuntimeException("Read the logminer offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new LogMinerOffset(Scn.valueOf(offset.get(SourceInfo.SCN_KEY)));
    }

    @Override
    public Offset specific(String filename, Long position) {
        throw new UnsupportedOperationException("not supported create new Offset by filename and position.");
    }

    @Override
    public Offset timestamp(long timestamp) {
        throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
    }
}
