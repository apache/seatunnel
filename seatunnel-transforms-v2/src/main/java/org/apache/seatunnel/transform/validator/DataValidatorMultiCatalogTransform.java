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

package org.apache.seatunnel.transform.validator;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogMapTransform;
import org.apache.seatunnel.transform.common.IdentityMapTransform;

import java.util.List;

/**
 * Multi-table wrapper of the {@link DataValidatorTransform}: every table matched by the
 * transform-level rule (or selected by a {@code table_transform} entry) is validated with its own
 * field rules, tables without a matching rule are passed through unchanged.
 */
public class DataValidatorMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

    public DataValidatorMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public String getPluginName() {
        return DataValidatorTransform.PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return new DataValidatorTransform(config, inputCatalogTable);
    }

    /** Mirrors the mandatory options of the transform-level rule declared by the factory. */
    @Override
    protected boolean hasTransformLevelRuleConfigured(ReadonlyConfig config) {
        return hasAnyOptionConfigured(config, DataValidatorTransformConfig.FIELD_RULES);
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(CatalogTable catalogTable) {
        return new IdentityMapTransform(catalogTable);
    }
}
