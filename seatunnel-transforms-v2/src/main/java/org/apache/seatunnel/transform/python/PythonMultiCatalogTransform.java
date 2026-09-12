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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogMapTransform;
import org.apache.seatunnel.transform.common.IdentityMapTransform;

import java.util.List;

/** Multi-table wrapper that instantiates one Python transform per matched catalog table. */
public class PythonMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

    /**
     * Creates the multi-table transform wrapper.
     *
     * @param inputCatalogTables input tables available to the pipeline
     * @param config readonly transform config
     */
    public PythonMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    /**
     * Returns the transform identifier shared by all wrapped tables.
     *
     * @return plugin name
     */
    @Override
    public String getPluginName() {
        return PythonTransform.PLUGIN_NAME;
    }

    /**
     * Builds the per-table Python transform used at runtime.
     *
     * @param inputCatalogTable matched input table
     * @param config readonly transform config
     * @return transform instance for the current table
     */
    @Override
    protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return new PythonTransform(inputCatalogTable, PythonTransformConfig.of(config));
    }

    /**
     * Preserves unmatched tables when multi-table routing skips the Python transform.
     *
     * @param catalogTable unmatched input table
     * @return identity transform for pass-through behavior
     */
    @Override
    protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(CatalogTable catalogTable) {
        return new IdentityMapTransform(catalogTable);
    }

    /** Closes every inner transform so Python subprocesses do not outlive the wrapper. */
    @Override
    public void close() {
        RuntimeException closeFailure = null;
        for (SeaTunnelTransform<SeaTunnelRow> transform : transformMap.values()) {
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
}
