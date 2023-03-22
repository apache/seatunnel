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

package org.apache.seatunnel.transform.fieldrowkind;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.transform.filterfield.FilterFieldTransform;
import org.apache.seatunnel.transform.filterfield.FilterFieldTransformConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.transform.fieldrowkind.FilterRowKindTransform.EXCLUDE_KINDS;
import static org.apache.seatunnel.transform.fieldrowkind.FilterRowKindTransform.INCLUDE_KINDS;

@AutoService(Factory.class)
public class FilterRowKindTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "FilterRowKind";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().exclusive(EXCLUDE_KINDS, INCLUDE_KINDS).build();
    }

    @Override
    public TableTransform createTransform(TableFactoryContext context) {
        FilterRowKinkTransformConfig config = FilterRowKinkTransformConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new FilterRowKindTransform(config, catalogTable);
    }
}
