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
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class SheetsSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "GoogleSheets";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SheetsSourceOptions.SERVICE_ACCOUNT_KEY)
                .required(SheetsSourceOptions.SHEET_ID)
                .required(SheetsSourceOptions.SHEET_NAME)
                .required(SheetsSourceOptions.RANGE)
                .optional(ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        CatalogTable catalogTable;
        if (context.getOptions().getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(context.getOptions());
        } else {
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
        }
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new SheetsSource(
                                catalogTable,
                                new SheetsParameters().buildWithConfig(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SheetsSource.class;
    }
}
