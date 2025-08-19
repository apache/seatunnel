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

package org.apache.seatunnel.connectors.seatunnel.dsql.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.dsql.config.DSQLSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.dsql.config.DSQLSinkOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DSQLSinkFactory implements TableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DSQLSinkFactory.class);

    @Override
    public String factoryIdentifier() {
        return "DSQL";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // Required options
                .required(
                        DSQLSinkOptions.CLUSTER_ENDPOINT,
                        DSQLSinkOptions.DATABASE_NAME,
                        DSQLSinkOptions.AWS_REGION)
                // Authentication options
                .optional(
                        DSQLSinkOptions.ACCESS_KEY_ID,
                        DSQLSinkOptions.SECRET_ACCESS_KEY,
                        DSQLSinkOptions.PROFILE_NAME)
                // Basic optional options
                .optional(
                        DSQLSinkOptions.TABLE_NAME,
                        DSQLSinkOptions.BATCH_SIZE,
                        DSQLSinkOptions.MAX_RETRIES,
                        DSQLSinkOptions.RETRY_DELAY_MS,
                        DSQLSinkOptions.CREATE_TABLE_IF_NOT_EXISTS)
                // Enhanced optional options
                .optional(
                        DSQLSinkOptions.CONNECTION_TIMEOUT_MS,
                        DSQLSinkOptions.SOCKET_TIMEOUT_MS,
                        DSQLSinkOptions.PRIMARY_KEYS,
                        DSQLSinkOptions.USE_SSL)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        LOG.info(
                "Creating DSQL sink for table: {}",
                context.getCatalogTable().getTableId().getTableName());

        try {
            // Create and validate the configuration
            DSQLSinkConfig dsqlConfig = new DSQLSinkConfig(config);

            // Log configuration details (excluding sensitive information)
            LOG.info(
                    "DSQL sink configuration: cluster={}, database={}, table={}, region={}, batchSize={}, writeMode={}",
                    dsqlConfig.getClusterEndpoint(),
                    dsqlConfig.getDatabaseName(),
                    dsqlConfig.getTableName(),
                    dsqlConfig.getAwsRegion(),
                    dsqlConfig.getBatchSize());

            return () -> new DSQLSink(config, context.getCatalogTable());
        } catch (Exception e) {
            LOG.error("Failed to create DSQL sink", e);
            throw e;
        }
    }
}
