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

package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_BEARER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_KERBEROS_TICKET;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_MAX_CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_MAX_TRANSACTION_RETRY_TIME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_NEO4J_URI;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.KEY_USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.PLUGIN_NAME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkConfig.QUERY_PARAM_POSITION;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.neo4j.driver.AuthTokens;

import java.io.IOException;
import java.net.URI;

@AutoService(SeaTunnelSink.class)
public class Neo4jSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    private SeaTunnelRowType rowType;
    private final Neo4jSinkQueryInfo neo4JSinkQueryInfo = new Neo4jSinkQueryInfo();

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        neo4JSinkQueryInfo.setDriverBuilder(prepareDriver(config));

        final CheckResult queryConfigCheck =
            CheckConfigUtil.checkAllExists(config, KEY_QUERY.key(), QUERY_PARAM_POSITION.key());
        if (!queryConfigCheck.isSuccess()) {
            throw new Neo4jConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    PLUGIN_NAME, PluginType.SINK, queryConfigCheck.getMsg()));
        }
        neo4JSinkQueryInfo.setQuery(config.getString(KEY_QUERY.key()));
        neo4JSinkQueryInfo.setQueryParamPosition(config.getObject(QUERY_PARAM_POSITION.key()).unwrapped());
    }

    private DriverBuilder prepareDriver(Config config) {
        final CheckResult uriConfigCheck =
            CheckConfigUtil.checkAllExists(config, KEY_NEO4J_URI.key(), KEY_DATABASE.key());
        final CheckResult authConfigCheck =
            CheckConfigUtil.checkAtLeastOneExists(config, KEY_USERNAME.key(), KEY_BEARER_TOKEN.key(),
                KEY_KERBEROS_TICKET.key());
        final CheckResult mergedConfigCheck = CheckConfigUtil.mergeCheckResults(uriConfigCheck, authConfigCheck);
        if (!mergedConfigCheck.isSuccess()) {
            throw new Neo4jConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    PLUGIN_NAME, PluginType.SINK, mergedConfigCheck.getMsg()));
        }

        final URI uri = URI.create(config.getString(KEY_NEO4J_URI.key()));

        final DriverBuilder driverBuilder = DriverBuilder.create(uri);

        if (config.hasPath(KEY_USERNAME.key())) {
            final CheckResult pwParamCheck = CheckConfigUtil.checkAllExists(config, KEY_PASSWORD.key());
            if (!pwParamCheck.isSuccess()) {
                throw new Neo4jConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                        PLUGIN_NAME, PluginType.SINK, pwParamCheck.getMsg()));
            }
            final String username = config.getString(KEY_USERNAME.key());
            final String password = config.getString(KEY_PASSWORD.key());

            driverBuilder.setUsername(username);
            driverBuilder.setPassword(password);
        } else if (config.hasPath(KEY_BEARER_TOKEN.key())) {
            final String bearerToken = config.getString(KEY_BEARER_TOKEN.key());
            AuthTokens.bearer(bearerToken);
            driverBuilder.setBearerToken(bearerToken);
        } else {
            final String kerberosTicket = config.getString(KEY_KERBEROS_TICKET.key());
            AuthTokens.kerberos(kerberosTicket);
            driverBuilder.setBearerToken(kerberosTicket);
        }

        driverBuilder.setDatabase(config.getString(KEY_DATABASE.key()));

        if (config.hasPath(KEY_MAX_CONNECTION_TIMEOUT.key())) {
            driverBuilder.setMaxConnectionTimeoutSeconds(config.getLong(KEY_MAX_CONNECTION_TIMEOUT.key()));
        }
        if (config.hasPath(KEY_MAX_TRANSACTION_RETRY_TIME.key())) {
            driverBuilder.setMaxTransactionRetryTimeSeconds(config.getLong(KEY_MAX_TRANSACTION_RETRY_TIME.key()));
        }

        return driverBuilder;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.rowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.rowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new Neo4jSinkWriter(neo4JSinkQueryInfo);
    }

}
