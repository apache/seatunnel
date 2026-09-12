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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * Pass-through map transform used by multi-table wrappers for tables that no rule matches. Its
 * produced table always mirrors its current input so that schema changes routed to it refresh the
 * produced table the wrapper reports downstream.
 */
public class IdentityMapTransform extends AbstractCatalogSupportMapTransform {

    public IdentityMapTransform(CatalogTable catalogTable) {
        super(catalogTable);
    }

    @Override
    public String getPluginName() {
        return "IdentityMap";
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow row) {
        return row;
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
            inputCatalogTable = adopt(inputCatalogTable, event);
            outputCatalogTable = null;
        }
        return event;
    }

    /**
     * Returns the input an identity transform must use after the event: the event's {@code
     * changeAfter} re-keyed to the transform's own table identifier when the source provided it,
     * otherwise the event applied to the current input.
     */
    static CatalogTable adopt(CatalogTable input, SchemaChangeEvent event) {
        CatalogTable changeAfter = event.getChangeAfter();
        if (changeAfter != null) {
            return CatalogTable.of(input.getTableId(), changeAfter);
        }
        return IdentityFlatMapTransform.applyToInput(input, event);
    }
}
