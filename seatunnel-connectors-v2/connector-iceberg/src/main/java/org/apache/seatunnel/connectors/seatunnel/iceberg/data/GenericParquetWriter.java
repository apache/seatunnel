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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.iceberg.data.parquet.BaseParquetWriter;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.parquet.schema.MessageType;

import java.util.List;

public class GenericParquetWriter extends BaseParquetWriter<SeaTunnelRow> {
    private static final GenericParquetWriter INSTANCE = new GenericParquetWriter();

    private GenericParquetWriter() {}

    public static ParquetValueWriter<SeaTunnelRow> buildWriter(MessageType type) {
        return INSTANCE.createWriter(type);
    }

    @Override
    protected StructWriter<SeaTunnelRow> createStructWriter(List<ParquetValueWriter<?>> writers) {
        return new SeaTunnelRowWriter(writers);
    }

    private static class SeaTunnelRowWriter extends StructWriter<SeaTunnelRow> {
        private SeaTunnelRowWriter(List<ParquetValueWriter<?>> writers) {
            super(writers);
        }

        @Override
        protected Object get(SeaTunnelRow struct, int index) {
            return struct.getField(index);
        }
    }
}
