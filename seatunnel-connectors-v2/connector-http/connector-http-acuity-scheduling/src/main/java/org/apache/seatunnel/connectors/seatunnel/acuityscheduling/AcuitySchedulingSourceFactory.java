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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.API_URL;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.CALENDAR_ID;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.DATE_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.LOOKBACK_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_REQUESTS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.RESUME_DATE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.TIMEZONE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.WINDOW_DAYS;

/** Registers the Acuity Scheduling source and validates its connection and traversal options. */
@AutoService(Factory.class)
public final class AcuitySchedulingSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AcuitySchedulingSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(USERNAME, PASSWORD)
                .optional(
                        API_URL,
                        RESOURCES,
                        START_DATE,
                        END_DATE,
                        RESUME_DATE,
                        TIMEZONE,
                        LOOKBACK_DAYS,
                        WINDOW_DAYS,
                        DATE_LIMIT,
                        CALENDAR_ID,
                        MAX_REQUESTS,
                        MAX_BYTES,
                        MAX_ROWS,
                        CONNECT_TIMEOUT,
                        READ_TIMEOUT,
                        REQUEST_TIMEOUT,
                        MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AcuitySchedulingSource source =
                new AcuitySchedulingSource(new AcuityConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AcuitySchedulingSource.class;
    }
}
