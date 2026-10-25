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
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class ChainedMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

    private final List<SeaTunnelTransform<SeaTunnelRow>> transforms;

    /** Extra table paths of every rule, rebuilt only by {@link #refreshExtraTablePaths()}. */
    private final List<Set<String>> extraTablePaths = new ArrayList<>();

    ChainedMapTransform(List<SeaTunnelTransform<SeaTunnelRow>> transforms) {
        this.transforms = transforms;
        // Initial resolution; refreshed whenever the input table of a rule changes.
        refreshExtraTablePaths();
    }

    /**
     * Rebuilds the extra table paths of every rule. Runs on construction and on schema changes: a
     * rule derives its produced tables from its input table, which only changes there.
     */
    private void refreshExtraTablePaths() {
        extraTablePaths.clear();
        for (SeaTunnelTransform<SeaTunnelRow> transform : transforms) {
            Set<String> paths = new HashSet<>();
            List<CatalogTable> producedTables = transform.getProducedCatalogTables();
            for (int i = 1; i < producedTables.size(); i++) {
                paths.add(producedTables.get(i).getTableId().toTablePath().toString());
            }
            extraTablePaths.add(paths);
        }
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
    public SeaTunnelRow map(SeaTunnelRow row) {
        SeaTunnelRow currentRow = row;
        for (int i = 0; i < transforms.size(); i++) {
            if (currentRow == null) {
                return null;
            }
            SeaTunnelTransform<SeaTunnelRow> transform = transforms.get(i);
            currentRow = ((SeaTunnelMapTransform<SeaTunnelRow>) transform).map(currentRow);
            // A transform may route a row to one of its extra tables (the DataValidator error
            // table): that row carries the schema of the extra table and must not be processed by
            // the remaining rules of the chain.
            if (currentRow != null && extraTablePaths.get(i).contains(currentRow.getTableId())) {
                return currentRow;
            }
        }
        return currentRow;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return transforms.get(transforms.size() - 1).getProducedCatalogTable();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> producedTables = new ArrayList<>();
        producedTables.add(getProducedCatalogTable());
        Set<String> producedTablePaths = new HashSet<>();
        producedTablePaths.add(getProducedCatalogTable().getTableId().toTablePath().toString());
        // The extra tables of every rule are part of the output of the chain, not only the ones of
        // the last rule.
        for (SeaTunnelTransform<SeaTunnelRow> transform : transforms) {
            List<CatalogTable> innerProducedTables = transform.getProducedCatalogTables();
            for (int i = 1; i < innerProducedTables.size(); i++) {
                CatalogTable extraTable = innerProducedTables.get(i);
                if (producedTablePaths.add(extraTable.getTableId().toTablePath().toString())) {
                    producedTables.add(extraTable);
                }
            }
        }
        return producedTables;
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        SchemaChangeEvent currentEvent = schemaChangeEvent;
        for (int i = 0; i < transforms.size(); i++) {
            SeaTunnelTransform<SeaTunnelRow> transform = transforms.get(i);
            currentEvent = transform.mapSchemaChangeEvent(currentEvent);
            if (currentEvent == null) {
                break;
            }
            if (i + 1 < transforms.size()) {
                setTransformInput(transforms.get(i + 1), transform.getProducedCatalogTable());
            }
        }
        // setTransformInput above re-pointed the rules the event reached, so refresh the routing
        // paths before the following rows are mapped.
        refreshExtraTablePaths();
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
        // The rules have new input tables and derive their produced tables from them, so refresh
        // the routing paths.
        refreshExtraTablePaths();
    }

    @Override
    public void close() {
        RuntimeException closeFailure = null;
        for (SeaTunnelTransform<SeaTunnelRow> transform : transforms) {
            try {
                transform.close();
            } catch (RuntimeException e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
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
