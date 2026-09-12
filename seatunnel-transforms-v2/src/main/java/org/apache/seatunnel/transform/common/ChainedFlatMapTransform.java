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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class ChainedFlatMapTransform implements SeaTunnelFlatMapTransform<SeaTunnelRow> {

    private final List<SeaTunnelTransform<SeaTunnelRow>> transforms;

    ChainedFlatMapTransform(List<SeaTunnelTransform<SeaTunnelRow>> transforms) {
        this.transforms = transforms;
    }

    @Override
    public String getPluginName() {
        return transforms.get(0).getPluginName();
    }

    @Override
    public void open() {
        transforms.forEach(SeaTunnelTransform::open);
    }

    @Override
    public List<SeaTunnelRow> flatMap(SeaTunnelRow row) {
        List<SeaTunnelRow> currentRows = Collections.singletonList(row);
        for (SeaTunnelTransform<SeaTunnelRow> transform : transforms) {
            List<SeaTunnelRow> nextRows = new ArrayList<>();
            for (SeaTunnelRow currentRow : currentRows) {
                List<SeaTunnelRow> rows =
                        ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(currentRow);
                if (rows != null) {
                    nextRows.addAll(rows);
                }
            }
            currentRows = nextRows;
        }
        return currentRows;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return transforms.get(transforms.size() - 1).getProducedCatalogTable();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(getProducedCatalogTable());
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        SchemaChangeEvent currentEvent = schemaChangeEvent;
        for (int i = 0; i < transforms.size(); i++) {
            SeaTunnelTransform<SeaTunnelRow> transform = transforms.get(i);
            currentEvent = transform.mapSchemaChangeEvent(currentEvent);
            if (currentEvent == null) {
                return null;
            }
            if (i + 1 < transforms.size()) {
                setTransformInput(transforms.get(i + 1), transform.getProducedCatalogTable());
            }
        }
        return currentEvent;
    }

    @Override
    public void setInputCatalogTables(List<CatalogTable> inputCatalogTables) {
        if (inputCatalogTables == null || inputCatalogTables.isEmpty()) {
            return;
        }
        CatalogTable currentCatalogTable = inputCatalogTables.get(0);
        for (SeaTunnelTransform<SeaTunnelRow> transform : transforms) {
            setTransformInput(transform, currentCatalogTable);
            currentCatalogTable = transform.getProducedCatalogTable();
        }
    }

    @Override
    public void close() {
        transforms.forEach(SeaTunnelTransform::close);
    }

    private void setTransformInput(
            SeaTunnelTransform<SeaTunnelRow> transform, CatalogTable catalogTable) {
        if (transform instanceof AbstractSeaTunnelTransform) {
            ((AbstractSeaTunnelTransform<?, ?>) transform).setInputCatalogTable(catalogTable);
        } else {
            transform.setInputCatalogTables(Collections.singletonList(catalogTable));
        }
    }
}
