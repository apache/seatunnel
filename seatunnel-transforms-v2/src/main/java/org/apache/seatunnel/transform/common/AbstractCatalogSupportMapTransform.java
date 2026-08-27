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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCatalogSupportMapTransform
        extends AbstractSeaTunnelTransform<SeaTunnelRow, SeaTunnelRow>
        implements SeaTunnelMapTransform<SeaTunnelRow> {
    public AbstractCatalogSupportMapTransform(@NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
    }

    public AbstractCatalogSupportMapTransform(
            @NonNull CatalogTable inputCatalogTable, ErrorHandleWay rowErrorHandleWay) {
        super(inputCatalogTable, rowErrorHandleWay);
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        return transform(row);
    }

    /**
     * Default schema-change handler for map transforms that cache field indices at init time.
     *
     * <p>Updates {@code inputCatalogTable} from the event, resets the output-schema cache, then
     * re-runs {@link #transformTableSchema()} so that any field-index arrays (e.g. {@code
     * inputValueIndexList} in {@code FilterFieldTransform}, {@code fieldsIndex} / {@code
     * rowContainerGenerator} in {@code MultipleFieldOutputTransform}) are rebuilt against the new
     * schema before the next data row arrives.
     *
     * <p>Subclasses that need special handling (e.g. {@code TableRenameTransform}, {@code
     * FieldRenameTransform}) continue to override this method; their overrides take precedence via
     * normal Java polymorphism and are not affected.
     */
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof AlterTableEvent) {
            TableSchema newSchema =
                    new AlterTableSchemaEventHandler()
                            .reset(inputCatalogTable.getTableSchema())
                            .apply(event);
            inputCatalogTable =
                    CatalogTable.of(
                            inputCatalogTable.getTableId(),
                            newSchema,
                            inputCatalogTable.getOptions(),
                            inputCatalogTable.getPartitionKeys(),
                            inputCatalogTable.getComment(),
                            inputCatalogTable.getTableId().getCatalogName(),
                            inputCatalogTable.getMetadataSchema());
            outputCatalogTable = null;
            // Rebuild all derived field-index state eagerly so the next transformRow() call
            // sees correct indices without needing a lazy trigger.
            transformTableSchema();
        }
        return event;
    }

    /** Eagerly rebuild any field-index/row-container caches that depend on inputCatalogTable. */
    @Override
    public void setInputCatalogTable(@NonNull CatalogTable inputCatalogTable) {
        super.setInputCatalogTable(inputCatalogTable);
        transformTableSchema();
    }
}
