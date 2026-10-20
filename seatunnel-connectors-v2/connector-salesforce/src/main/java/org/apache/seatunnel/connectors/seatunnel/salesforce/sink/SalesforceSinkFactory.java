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

package org.apache.seatunnel.connectors.seatunnel.salesforce.sink;

import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSourceOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SalesforceSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Salesforce";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        SalesforceSourceOptions.CLIENT_ID,
                        SalesforceSourceOptions.CLIENT_SECRET,
                        SalesforceSourceOptions.USERNAME,
                        SalesforceSourceOptions.PASSWORD,
                        SalesforceSourceOptions.INSTANCE_URL)
                .required(
                        SalesforceSourceOptions.OBJECT_NAME,
                        Conditions.matches(
                                SalesforceSourceOptions.OBJECT_NAME,
                                SalesforceSinkConfig.IDENTIFIER_PATTERN))
                .required(
                        SalesforceSinkOptions.EXTERNAL_ID_FIELD,
                        Conditions.matches(
                                SalesforceSinkOptions.EXTERNAL_ID_FIELD,
                                SalesforceSinkConfig.IDENTIFIER_PATTERN))
                .optional(
                        SalesforceSourceOptions.SECURITY_TOKEN, SalesforceSourceOptions.API_VERSION)
                .optional(
                        SalesforceSourceOptions.REQUEST_TIMEOUT_MS,
                        Conditions.greaterThan(SalesforceSourceOptions.REQUEST_TIMEOUT_MS, 0))
                .optional(
                        SalesforceSinkOptions.BATCH_SIZE,
                        Conditions.greaterThan(SalesforceSinkOptions.BATCH_SIZE, 0),
                        Conditions.lessThan(SalesforceSinkOptions.BATCH_SIZE, 201))
                .optional(
                        SalesforceSinkOptions.BATCH_MAX_BYTES,
                        Conditions.greaterThan(SalesforceSinkOptions.BATCH_MAX_BYTES, 127),
                        Conditions.lessThan(
                                SalesforceSinkOptions.BATCH_MAX_BYTES, 8 * 1024 * 1024 + 1))
                .optional(
                        SalesforceSinkOptions.MAX_RETRIES,
                        Conditions.greaterThan(SalesforceSinkOptions.MAX_RETRIES, -1),
                        Conditions.lessThan(SalesforceSinkOptions.MAX_RETRIES, 11))
                .optional(
                        SalesforceSinkOptions.RETRY_INTERVAL_MS,
                        Conditions.greaterThan(SalesforceSinkOptions.RETRY_INTERVAL_MS, -1L),
                        Conditions.lessThan(SalesforceSinkOptions.RETRY_INTERVAL_MS, 60001L))
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        SalesforceSinkConfig config = new SalesforceSinkConfig(context.getOptions());
        // Fail during planning; the serializer also validates direct writer construction.
        SalesforceRowSerializer.validateSchema(
                context.getCatalogTable().getSeaTunnelRowType(), config);
        return () -> new SalesforceSink(config, context.getCatalogTable());
    }
}
