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

package org.apache.seatunnel.transform.rename;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.transform.rename.TableRenameConfig.CONVERT_CASE;
import static org.apache.seatunnel.transform.rename.TableRenameConfig.PREFIX;
import static org.apache.seatunnel.transform.rename.TableRenameConfig.REPLACEMENTS_WITH_REGEX;
import static org.apache.seatunnel.transform.rename.TableRenameConfig.SUFFIX;

@AutoService(Factory.class)
public class TableRenameTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return TableRenameTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(CONVERT_CASE, PREFIX, SUFFIX, REPLACEMENTS_WITH_REGEX)
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new TableRenameMultiCatalogTransform(
                        context.getCatalogTables(), context.getOptions());
    }
}
