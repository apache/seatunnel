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

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
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
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.neo4j.driver.AuthTokens;

import java.net.URI;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig.*;

@AutoService(SeaTunnelSource.class)
public class Neo4jSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final Neo4jSourceConfig neo4jSourceConfig = new Neo4jSourceConfig();
    private SeaTunnelRowType rowType;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        neo4jSourceConfig.setDriverBuilder(prepareDriver(pluginConfig));

        final CheckResult queryConfigCheck = CheckConfigUtil.checkAllExists(pluginConfig, KEY_QUERY);
        if (!queryConfigCheck.isSuccess()) {
            throw new PrepareFailException(Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, queryConfigCheck.getMsg());
        }
        neo4jSourceConfig.setQuery(pluginConfig.getString(KEY_QUERY));

        final CheckResult schemaConfigCheck = CheckConfigUtil.checkAllExists(pluginConfig, SeaTunnelSchema.SCHEMA);
        if (!schemaConfigCheck.isSuccess()) {
            throw new PrepareFailException(Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, schemaConfigCheck.getMsg());
        }
        this.rowType = SeaTunnelSchema.buildWithConfig(pluginConfig.getConfig(SeaTunnelSchema.SCHEMA)).getSeaTunnelRowType();
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
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new Neo4jSourceReader(readerContext, neo4jSourceConfig, rowType);
    }

    private DriverBuilder prepareDriver(Config config) {
        final CheckResult uriConfigCheck = CheckConfigUtil.checkAllExists(config, KEY_NEO4J_URI, KEY_DATABASE);
        final CheckResult authConfigCheck = CheckConfigUtil.checkAtLeastOneExists(config, KEY_USERNAME, KEY_BEARER_TOKEN, KEY_KERBEROS_TICKET);
        final CheckResult mergedConfigCheck = CheckConfigUtil.mergeCheckResults(uriConfigCheck, authConfigCheck);
        if (!mergedConfigCheck.isSuccess()) {
            throw new PrepareFailException(Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, mergedConfigCheck.getMsg());
        }

        final URI uri = URI.create(config.getString(KEY_NEO4J_URI));
        if (!"neo4j".equals(uri.getScheme())) {
            throw new PrepareFailException(Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, "uri scheme is not `neo4j`");
        }

        final DriverBuilder driverBuilder = DriverBuilder.create(uri);

        if (config.hasPath(KEY_USERNAME)) {
            final CheckResult pwParamCheck = CheckConfigUtil.checkAllExists(config, KEY_PASSWORD);
            if (!mergedConfigCheck.isSuccess()) {
                throw new PrepareFailException(Neo4jSourceConfig.PLUGIN_NAME, PluginType.SOURCE, pwParamCheck.getMsg());
            }
            final String username = config.getString(KEY_USERNAME);
            final String password = config.getString(KEY_PASSWORD);

            driverBuilder.setUsername(username);
            driverBuilder.setPassword(password);
        } else if (config.hasPath(KEY_BEARER_TOKEN)) {
            final String bearerToken = config.getString(KEY_BEARER_TOKEN);
            AuthTokens.bearer(bearerToken);
            driverBuilder.setBearerToken(bearerToken);
        } else {
            final String kerberosTicket = config.getString(KEY_KERBEROS_TICKET);
            AuthTokens.kerberos(kerberosTicket);
            driverBuilder.setBearerToken(kerberosTicket);
        }

        driverBuilder.setDatabase(config.getString(KEY_DATABASE));

        if (config.hasPath(KEY_MAX_CONNECTION_TIMEOUT)) {
            driverBuilder.setMaxConnectionTimeoutSeconds(config.getLong(KEY_MAX_CONNECTION_TIMEOUT));
        }
        if (config.hasPath(KEY_MAX_TRANSACTION_RETRY_TIME)) {
            driverBuilder.setMaxTransactionRetryTimeSeconds(config.getLong(KEY_MAX_TRANSACTION_RETRY_TIME));
        }

        return driverBuilder;
    }
}
