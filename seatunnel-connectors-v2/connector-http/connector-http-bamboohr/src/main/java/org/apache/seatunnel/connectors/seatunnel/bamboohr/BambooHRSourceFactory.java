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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.COMPANY;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.COMPANY_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.CUSTOM_REPORTS_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.EMPLOYEE_CHANGED_SINCE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.EMPLOYEE_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.MAX_RECORDS_PER_RESPONSE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.TIMESHEET_WATERMARK;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.TIME_OFF_WATERMARK;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.TOKEN;

/**
 * Registers and validates the BambooHR V2 table source.
 *
 * <p>The factory exposes the same identifier as the connector installation mapping.
 */
@AutoService(Factory.class)
public final class BambooHRSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return BambooHRSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(COMPANY, TOKEN, RESOURCE)
                .optional(
                        CUSTOM_REPORTS_FIELDS,
                        EMPLOYEE_FIELDS,
                        START_DATE,
                        END_DATE,
                        COMPANY_TIME_ZONE,
                        TIME_OFF_WATERMARK,
                        TIMESHEET_WATERMARK,
                        EMPLOYEE_CHANGED_SINCE,
                        CONNECT_TIMEOUT_MS,
                        READ_TIMEOUT_MS,
                        REQUEST_DEADLINE_MS,
                        MAX_ATTEMPTS,
                        MAX_RESPONSE_BYTES,
                        MAX_RECORDS_PER_RESPONSE)
                .build();
    }

    /**
     * Validates options before constructing a catalog table or creating network readers.
     *
     * <p>Invalid option combinations therefore fail before the engine contacts the provider.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        BambooHRConfig config = new BambooHRConfig(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new BambooHRSource(config);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return BambooHRSource.class;
    }
}
