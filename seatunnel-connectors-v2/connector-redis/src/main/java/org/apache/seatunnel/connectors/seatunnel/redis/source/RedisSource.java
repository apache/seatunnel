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

package org.apache.seatunnel.connectors.seatunnel.redis.source;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.util.List;

public class RedisSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final RedisParameters redisParameters = new RedisParameters();
    private SeaTunnelRowType seaTunnelRowType;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private CatalogTable catalogTable;

    @Override
    public String getPluginName() {
        return RedisBaseOptions.CONNECTOR_IDENTITY;
    }

    public RedisSource(ReadonlyConfig readonlyConfig) {

        this.redisParameters.buildWithConfig(readonlyConfig);
        // TODO: use format SPI
        // default use json format
        if (readonlyConfig.getOptional(RedisBaseOptions.FORMAT).isPresent()) {
            if (!readonlyConfig.getOptional(SinkConnectorCommonOptions.SCHEMA).isPresent()) {
                throw new RedisConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "PluginName: %s, PluginType: %s, Message: %s",
                                getPluginName(),
                                PluginType.SOURCE,
                                "Must config schema when format parameter been config"));
            }

            RedisBaseOptions.Format format = readonlyConfig.get(RedisBaseOptions.FORMAT);
            if (RedisBaseOptions.Format.JSON.equals(format)) {
                this.catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
                this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
                this.deserializationSchema =
                        new JsonDeserializationSchema(catalogTable, false, false);
            }
        } else {
            this.catalogTable = CatalogTableUtil.buildSimpleTextTable();
            this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
            this.deserializationSchema = null;
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new RedisSourceReader(redisParameters, readerContext, deserializationSchema);
    }
}
