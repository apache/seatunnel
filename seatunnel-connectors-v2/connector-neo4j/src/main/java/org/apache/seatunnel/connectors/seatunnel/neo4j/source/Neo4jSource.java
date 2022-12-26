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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_BEARER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_KERBEROS_TICKET;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_MAX_CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_MAX_TRANSACTION_RETRY_TIME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_NEO4J_URI;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.KEY_USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.PLUGIN_NAME;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.neo4j.driver.AuthTokens;

import java.net.URI;

@AutoService(SeaTunnelSource.class)
public class Neo4jSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final Neo4jSourceQueryInfo neo4jSourceQueryInfo = new Neo4jSourceQueryInfo();
    private SeaTunnelRowType rowType;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        neo4jSourceQueryInfo.setDriverBuilder(prepareDriver(pluginConfig));

        final CheckResult configCheck =
            CheckConfigUtil.checkAllExists(pluginConfig, KEY_QUERY.key(), SeaTunnelSchema.SCHEMA.key());

        if (!configCheck.isSuccess()) {
            throw new Neo4jConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, configCheck.getMsg()));
        }
        neo4jSourceQueryInfo.setQuery(pluginConfig.getString(KEY_QUERY.key()));

        this.rowType =
            SeaTunnelSchema.buildWithConfig(pluginConfig.getConfig(SeaTunnelSchema.SCHEMA.key())).getSeaTunnelRowType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext)
        throws Exception {
        return new Neo4jSourceReader(readerContext, neo4jSourceQueryInfo, rowType);
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
                    Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, mergedConfigCheck.getMsg()));
        }

        final URI uri = URI.create(config.getString(KEY_NEO4J_URI.key()));

        final DriverBuilder driverBuilder = DriverBuilder.create(uri);

        if (config.hasPath(KEY_USERNAME.key())) {
            final CheckResult pwParamCheck = CheckConfigUtil.checkAllExists(config, KEY_PASSWORD.key());
            if (!pwParamCheck.isSuccess()) {
                throw new Neo4jConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                        Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, pwParamCheck.getMsg()));
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
}
