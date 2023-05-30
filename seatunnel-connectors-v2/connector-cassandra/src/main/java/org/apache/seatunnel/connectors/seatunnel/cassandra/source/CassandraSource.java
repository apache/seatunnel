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

package org.apache.seatunnel.connectors.seatunnel.cassandra.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig.CQL;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraConfig.KEYSPACE;

@AutoService(SeaTunnelSource.class)
public class CassandraSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private SeaTunnelRowType rowTypeInfo;
    private final CassandraParameters cassandraParameters = new CassandraParameters();

    @Override
    public String getPluginName() {
        return "Cassandra";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(pluginConfig, HOST.key(), KEYSPACE.key(), CQL.key());
        if (!checkResult.isSuccess()) {
            throw new CassandraConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, checkResult.getMsg()));
        }
        this.cassandraParameters.buildWithConfig(pluginConfig);
        try (CqlSession currentSession =
                CassandraClient.getCqlSessionBuilder(
                                pluginConfig.getString(HOST.key()),
                                pluginConfig.getString(KEYSPACE.key()),
                                cassandraParameters.getUsername(),
                                cassandraParameters.getPassword(),
                                cassandraParameters.getDatacenter())
                        .build()) {
            Row rs =
                    currentSession
                            .execute(
                                    CassandraClient.createSimpleStatement(
                                            pluginConfig.getString(CQL.key()),
                                            cassandraParameters.getConsistencyLevel()))
                            .one();
            if (rs == null) {
                throw new CassandraConnectorException(
                        CassandraConnectorErrorCode.NO_DATA_IN_SOURCE_TABLE,
                        "No data select from this cql: " + pluginConfig.getConfig(CQL.key()));
            }
            int columnSize = rs.getColumnDefinitions().size();
            String[] fieldNames = new String[columnSize];
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];
            for (int i = 0; i < columnSize; i++) {
                fieldNames[i] = rs.getColumnDefinitions().get(i).getName().asInternal();
                seaTunnelDataTypes[i] =
                        TypeConvertUtil.convert(rs.getColumnDefinitions().get(i).getType());
            }
            this.rowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        } catch (Exception e) {
            throw new CassandraConnectorException(
                    CommonErrorCode.TABLE_SCHEMA_GET_FAILED,
                    "Get table schema from cassandra source data failed",
                    e);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new CassandraSourceReader(cassandraParameters, readerContext);
    }
}
