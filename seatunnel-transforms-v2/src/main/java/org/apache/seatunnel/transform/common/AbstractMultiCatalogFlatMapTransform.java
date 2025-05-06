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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/** Abstract class for multi-table flat map transform. */
@Slf4j
public abstract class AbstractMultiCatalogFlatMapTransform extends AbstractMultiCatalogTransform
        implements SeaTunnelFlatMapTransform<SeaTunnelRow> {

    public AbstractMultiCatalogFlatMapTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public List<SeaTunnelRow> flatMap(SeaTunnelRow row) {
        String tableId;
        SeaTunnelFlatMapTransform<SeaTunnelRow> transform;
        if (transformMap.size() == 1) {
            tableId = transformMap.keySet().iterator().next();
        } else {
            tableId = row.getTableId();
        }
        transform = (SeaTunnelFlatMapTransform<SeaTunnelRow>) transformMap.get(tableId);

        try {
            return transform.flatMap(row);
        } catch (Exception e) {
            String message =
                    String.format(
                            "The transform %s map table %s data throw an error",
                            transform.getPluginName(), tableId);
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
