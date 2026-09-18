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

package org.apache.seatunnel.benchmark.transform;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;

import java.util.Collections;
import java.util.List;

/** Adds a deterministic CPU load and checksum while preserving the benchmark schema. */
public final class BenchmarkTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

    public static final String PLUGIN_NAME = "BenchmarkTransform";

    private CatalogTable catalogTable;
    private final int operationsPerRow;
    private final boolean copyRow;

    public BenchmarkTransform(CatalogTable catalogTable, int operationsPerRow, boolean copyRow) {
        this.catalogTable = catalogTable;
        this.operationsPerRow = operationsPerRow;
        this.copyRow = copyRow;
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow inputRow) {
        long sequence = (Long) inputRow.getField(0);
        String payload = (String) inputRow.getField(2);
        long hash = sequence ^ 0x9E3779B97F4A7C15L;
        int payloadLength = payload.length();
        for (int operation = 0; operation < operationsPerRow; operation++) {
            long value =
                    payloadLength == 0
                            ? operation
                            : payload.charAt(operation % payloadLength) + (long) operation;
            hash ^= value;
            hash *= 0xC2B2AE3D27D4EB4FL;
            hash = Long.rotateLeft(hash, 17);
        }

        SeaTunnelRow outputRow = copyRow ? inputRow.copy() : inputRow;
        outputRow.setField(3, hash);
        return outputRow;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return catalogTable;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public void setInputCatalogTables(List<CatalogTable> inputCatalogTables) {
        if (inputCatalogTables.size() != 1) {
            throw new IllegalArgumentException(
                    "BenchmarkTransform requires exactly one input table");
        }
        catalogTable = inputCatalogTables.get(0);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
}
