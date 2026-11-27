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

package org.apache.seatunnel.e2e.connector.fake;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Optional;

/**
 * Synchronous single-table test sink admitted by dirty-data planning validation. Deliberately does
 * not implement {@code SupportMultiTableSink}.
 */
public class TestSingleTableSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    private static final long serialVersionUID = 1L;

    private final CatalogTable catalogTable;

    public TestSingleTableSink(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return TestSingleTableSinkFactory.IDENTIFIER;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new TestSingleTableSinkWriter();
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
