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
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

/**
 * Pass-through flat-map transform used by multi-table wrappers for tables that no rule matches. Its
 * produced table always mirrors its current input so that schema changes routed to it refresh the
 * produced table the wrapper reports downstream.
 */
public class IdentityFlatMapTransform extends AbstractCatalogSupportFlatMapTransform {

    public IdentityFlatMapTransform(CatalogTable catalogTable) {
        super(catalogTable);
    }

    @Override
    public String getPluginName() {
        return "IdentityFlatMap";
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow row) {
        return Collections.singletonList(row);
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    /**
     * Adopts the post-change table carried by the event when present, otherwise applies the event
     * to the current input, and refreshes the produced table. Adopting {@code changeAfter} never
     * re-applies a change to an input that already reflects it, which matters when the engine hands
     * this transform the upstream produced table before dispatching the same event.
     *
     * @param event the upstream event
     * @return the event, forwarded unchanged
     */
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof AlterTableEvent) {
            inputCatalogTable = IdentityMapTransform.adopt(inputCatalogTable, event);
            outputCatalogTable = null;
        }
        return event;
    }

    static CatalogTable applyToInput(CatalogTable input, SchemaChangeEvent event) {
        TableSchema newSchema =
                new AlterTableSchemaEventHandler().reset(input.getTableSchema()).apply(event);
        return CatalogTable.of(
                input.getTableId(),
                newSchema,
                input.getOptions(),
                input.getPartitionKeys(),
                input.getComment(),
                input.getTableId().getCatalogName(),
                input.getMetadataSchema());
    }
}
