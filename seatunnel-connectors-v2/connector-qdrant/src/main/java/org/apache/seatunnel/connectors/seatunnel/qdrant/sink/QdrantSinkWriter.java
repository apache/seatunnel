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

package org.apache.seatunnel.connectors.seatunnel.qdrant.sink;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantParameters;

import java.io.IOException;
import java.util.Optional;

public class QdrantSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final QdrantBatchWriter batchWriter;

    public QdrantSinkWriter(CatalogTable catalog, QdrantParameters qdrantParameters) {
        int batchSize = 64;
        this.batchWriter = new QdrantBatchWriter(catalog, batchSize, qdrantParameters);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        batchWriter.addToBatch(element);
        if (batchWriter.needFlush()) {
            batchWriter.flush();
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        batchWriter.flush();
        return Optional.empty();
    }

    private void clearBuffer() {}

    @Override
    public void close() throws IOException {
        batchWriter.flush();
        batchWriter.close();
    }
}
