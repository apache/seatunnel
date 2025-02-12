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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SheetsSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "GoogleSheets";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SheetsConfig.SERVICE_ACCOUNT_KEY)
                .required(SheetsConfig.SHEET_ID)
                .required(SheetsConfig.SHEET_NAME)
                .required(SheetsConfig.RANGE)
                .optional(ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SheetsSource.class;
    }
}
