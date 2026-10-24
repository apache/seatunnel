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

package org.apache.seatunnel.connectors.seatunnel.couchbase.source;

import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.couchbase.config.CouchbaseSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.configuration.util.Conditions.greaterThan;
import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;

@AutoService(Factory.class)
public class CouchbaseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CouchbaseSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CouchbaseSource.class;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        CouchbaseSourceOptions.CONNECTION_STRING,
                        notBlank(CouchbaseSourceOptions.CONNECTION_STRING))
                .required(
                        CouchbaseSourceOptions.USERNAME, notBlank(CouchbaseSourceOptions.USERNAME))
                .required(CouchbaseSourceOptions.PASSWORD)
                .required(CouchbaseSourceOptions.BUCKET, notBlank(CouchbaseSourceOptions.BUCKET))
                .required(
                        CouchbaseSourceOptions.COLLECTION,
                        notBlank(CouchbaseSourceOptions.COLLECTION))
                .required(CouchbaseSourceOptions.SCHEMA)
                .optional(CouchbaseSourceOptions.SCOPE, notBlank(CouchbaseSourceOptions.SCOPE))
                .optional(
                        CouchbaseSourceOptions.READY_TIMEOUT,
                        greaterThan(CouchbaseSourceOptions.READY_TIMEOUT, 0))
                .optional(
                        CouchbaseSourceOptions.QUERY_TIMEOUT,
                        greaterThan(CouchbaseSourceOptions.QUERY_TIMEOUT, 0))
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ConfigValidator.of(context.getOptions()).validate(optionRule());
        CatalogTable table =
                CatalogTableUtil.buildWithConfig(factoryIdentifier(), context.getOptions());
        if (table.getSeaTunnelRowType().getTotalFields() == 0) {
            throw new OptionValidationException(
                    "Couchbase source requires non-empty schema.fields");
        }
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new CouchbaseSource(context.getOptions(), table);
    }
}
