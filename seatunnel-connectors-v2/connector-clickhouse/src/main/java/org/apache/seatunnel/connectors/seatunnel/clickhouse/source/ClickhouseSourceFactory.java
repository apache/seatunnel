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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(Factory.class)
public class ClickhouseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, SQL, USERNAME, PASSWORD)
                .optional(SERVER_TIME_ZONE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ClickhouseSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context)
                    throws PrepareFailException {
        ReadonlyConfig config = context.getOptions();

        List<ClickHouseNode> servers =
                ClickhouseUtil.createNodes(
                        config.get(HOST),
                        config.get(DATABASE),
                        config.get(SERVER_TIME_ZONE),
                        config.get(USERNAME),
                        config.get(PASSWORD));

        String sql = config.get(SQL);
        ClickHouseNode currentServer =
                servers.get(ThreadLocalRandom.current().nextInt(servers.size()));

        SeaTunnelRowType rowType;
        try (ClickHouseClient client = ClickHouseClient.newInstance(currentServer.getProtocol());
                ClickHouseResponse response =
                        client.connect(currentServer)
                                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                                .query(modifySQLToLimit1(sql))
                                .executeAndWait()) {

            int columnSize = response.getColumns().size();
            String[] fieldNames = new String[columnSize];
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];

            for (int i = 0; i < columnSize; i++) {
                fieldNames[i] = response.getColumns().get(i).getColumnName();
                seaTunnelDataTypes[i] = TypeConvertUtil.convert(response.getColumns().get(i));
            }

            rowType = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            factoryIdentifier(), PluginType.SOURCE, e.getMessage()));
        }

        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new ClickhouseSource(servers, rowType, sql);
    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) s LIMIT 1", sql);
    }
}
