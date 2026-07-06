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

package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;

import lombok.NonNull;

/** Row transform that delegates programmable field enrichment to a persistent Python worker. */
public class PythonTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "Python";

    /** Immutable configuration parsed from the job definition. */
    private final PythonTransformConfig transformConfig;

    /** Output columns appended to the input schema. */
    private final Column[] outputColumns;

    /** Lazily created Python process bound to this transform instance. */
    private transient PythonProcessWorker processWorker;

    /**
     * Creates one Python transform for a single catalog table.
     *
     * @param inputCatalogTable source schema seen by this transform
     * @param transformConfig normalized transform configuration
     */
    public PythonTransform(
            @NonNull CatalogTable inputCatalogTable,
            @NonNull PythonTransformConfig transformConfig) {
        super(inputCatalogTable, transformConfig.getErrorHandleWay());
        this.transformConfig = transformConfig;
        this.outputColumns =
                transformConfig.getColumnConfigs().stream()
                        .map(PythonColumnConfig::getDestColumn)
                        .toArray(Column[]::new);
    }

    /**
     * Returns the transform name exposed in job configs.
     *
     * @return plugin name
     */
    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    /** Starts the external Python worker before the first row is processed. */
    @Override
    public void open() {
        getProcessWorker().open();
    }

    /** Shuts down the external worker and releases temporary script files. */
    @Override
    public void close() {
        invalidateProcessWorker();
    }

    /**
     * Delegates row processing to the Python worker and returns only the appended fields.
     *
     * @param inputRow current input row accessor
     * @return output field values produced by Python
     */
    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        return getProcessWorker().processRow(inputRow);
    }

    /**
     * Drops the cached worker after schema changes so the next open rebuilds Python-side field
     * metadata and JSON converters against the new input table layout.
     *
     * @param event upstream schema change event
     * @return event forwarded downstream unchanged
     */
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        SchemaChangeEvent mappedEvent = super.mapSchemaChangeEvent(event);
        invalidateProcessWorker();
        return mappedEvent;
    }

    /**
     * Replaces the cached worker when the engine refreshes this transform from upstream's new
     * produced catalog.
     *
     * @param inputCatalogTable latest upstream catalog table
     */
    @Override
    public void setInputCatalogTable(@NonNull CatalogTable inputCatalogTable) {
        super.setInputCatalogTable(inputCatalogTable);
        invalidateProcessWorker();
    }

    /**
     * Returns the columns appended to the produced schema.
     *
     * @return output columns
     */
    @Override
    protected Column[] getOutputColumns() {
        return outputColumns;
    }

    /**
     * Creates the worker lazily so planning does not require a local Python runtime.
     *
     * @return worker bound to this transform instance
     */
    private PythonProcessWorker getProcessWorker() {
        if (processWorker == null) {
            processWorker = new PythonProcessWorker(transformConfig, inputCatalogTable);
        }
        return processWorker;
    }

    /**
     * Closes and discards the current worker so the next row rebuilds it from fresh schema state.
     */
    private void invalidateProcessWorker() {
        if (processWorker != null) {
            processWorker.close();
            processWorker = null;
        }
    }
}
