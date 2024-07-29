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
package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.translation.serialization.RowConverter;
import org.apache.seatunnel.translation.spark.execution.ColumnWithIndex;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;
import org.apache.seatunnel.translation.spark.utils.SchemaUtil;

import org.apache.spark.sql.catalyst.InternalRow;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class SeaTunnelSparkDataMultiWriter extends SeaTunnelSparkDataWriter {

    private final Map<String, RowConverter<InternalRow>> rowConverterMap;

    public SeaTunnelSparkDataMultiWriter(
            SinkWriter sinkWriter,
            @Nullable SinkCommitter sinkCommitter,
            CatalogTable[] catalogTables,
            long epochId) {
        super(sinkWriter, sinkCommitter, catalogTables[0], epochId);
        ColumnWithIndex[] columnWithIndexes = SchemaUtil.mergeSchema(Arrays.asList(catalogTables));
        CatalogTable mergeCatalogTable = columnWithIndexes[0].getMergeCatalogTable();
        this.rowConverterMap =
                Arrays.stream(columnWithIndexes)
                        .collect(
                                Collectors.toMap(
                                        columnWithIndex ->
                                                columnWithIndex
                                                        .getCatalogTable()
                                                        .getTablePath()
                                                        .toString(),
                                        columnWithIndex ->
                                                new InternalRowConverter(
                                                        mergeCatalogTable.getSeaTunnelRowType(),
                                                        columnWithIndex.getIndex())));
    }

    @Override
    public void write(InternalRow record) throws IOException {
        String tableId = record.getString(1);
        sinkWriter.write(rowConverterMap.get(tableId).reconvert(record));
    }
}
