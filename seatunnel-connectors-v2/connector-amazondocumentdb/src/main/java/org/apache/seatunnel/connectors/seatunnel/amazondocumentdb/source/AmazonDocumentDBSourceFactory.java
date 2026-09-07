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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.MATCH_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.PROJECTION;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.TLS;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.TLS_CA_FILE;
import static org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBSourceOptions.URI;

/** Factory entry point discovered through AutoService under the {@code AmazonDocumentDB} name. */
@AutoService(Factory.class)
public class AmazonDocumentDBSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "AmazonDocumentDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URI, DATABASE, COLLECTION, SCHEMA)
                .optional(TLS, TLS_CA_FILE, MATCH_QUERY, PROJECTION)
                .optional(FETCH_SIZE, Conditions.greaterThan(FETCH_SIZE, 0))
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AmazonDocumentDBSource(
                                new AmazonDocumentDBConfig(context.getOptions()),
                                CatalogTableUtil.buildWithConfig(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AmazonDocumentDBSource.class;
    }
}
