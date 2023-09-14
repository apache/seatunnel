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

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.time.LocalTime;
import java.util.List;
import java.util.stream.Stream;

public class GenericOrcWriter implements OrcRowWriter<SeaTunnelRow> {
    private final RecordWriter writer;

    private GenericOrcWriter(
            Schema expectedSchema, TypeDescription orcSchema, SeaTunnelRowType seaTunnelRowType) {
        Preconditions.checkArgument(
                orcSchema.getCategory() == TypeDescription.Category.STRUCT,
                "Top level must be a struct " + orcSchema);

        writer =
                (RecordWriter)
                        OrcSchemaWithTypeVisitor.visit(
                                expectedSchema, orcSchema, new WriteBuilder(seaTunnelRowType));
    }

    public static OrcRowWriter<SeaTunnelRow> buildWriter(
            Schema expectedSchema, TypeDescription fileSchema, SeaTunnelRowType seaTunnelRowType) {
        return new GenericOrcWriter(expectedSchema, fileSchema, seaTunnelRowType);
    }

    private static class WriteBuilder extends OrcSchemaWithTypeVisitor<OrcValueWriter<?>> {
        SeaTunnelRowType seaTunnelRowType;

        private WriteBuilder(SeaTunnelRowType seaTunnelRowType) {
            this.seaTunnelRowType = seaTunnelRowType;
        }

        @Override
        public OrcValueWriter<SeaTunnelRow> record(
                Types.StructType iStruct,
                TypeDescription record,
                List<String> names,
                List<OrcValueWriter<?>> fields) {
            return new RecordWriter(fields);
        }

        @Override
        public OrcValueWriter<?> list(
                Types.ListType iList, TypeDescription array, OrcValueWriter<?> element) {
            return GenericOrcWriters.list(element);
        }

        @Override
        public OrcValueWriter<?> map(
                Types.MapType iMap,
                TypeDescription map,
                OrcValueWriter<?> key,
                OrcValueWriter<?> value) {
            return GenericOrcWriters.map(key, value);
        }

        @Override
        public OrcValueWriter<?> primitive(
                Type.PrimitiveType iPrimitive, TypeDescription primitive) {
            SqlType sqlType = seaTunnelRowType.getFieldTypes()[primitive.getId() - 1].getSqlType();

            switch (sqlType) {
                case STRING:
                    return GenericOrcWriters.strings();
                case BOOLEAN:
                    return GenericOrcWriters.booleans();
                case TINYINT:
                    return GenericOrcWriters.bytes();
                case SMALLINT:
                    return GenericOrcWriters.shorts();
                case INT:
                    return GenericOrcWriters.ints();
                case BIGINT:
                    return GenericOrcWriters.longs();
                case FLOAT:
                    return GenericOrcWriters.floats(ORCSchemaUtil.fieldId(primitive));
                case DOUBLE:
                    return GenericOrcWriters.doubles(ORCSchemaUtil.fieldId(primitive));
                case DECIMAL:
                    Types.DecimalType decimalType = (Types.DecimalType) iPrimitive;
                    return GenericOrcWriters.decimal(decimalType.precision(), decimalType.scale());
                case BYTES:
                    return GenericOrcWriters.byteArrays();
                case DATE:
                    return GenericOrcWriters.dates();
                case TIME:
                    return TimeWriter.INSTANCE;
                case TIMESTAMP:
                    return GenericOrcWriters.timestamp();

                default:
                    throw new UnsupportedOperationException("Unsupported type: " + sqlType);
            }
        }
    }

    private static class TimeWriter implements OrcValueWriter<LocalTime> {
        private static final OrcValueWriter<LocalTime> INSTANCE = new TimeWriter();

        @Override
        public void nonNullWrite(int rowId, LocalTime data, ColumnVector output) {
            TimestampColumnVector cv = (TimestampColumnVector) output;
            cv.setIsUTC(true);
            cv.time[rowId] = data.toNanoOfDay() / 1_000;
            cv.nanos[rowId] = (int) (data.toNanoOfDay() % 1_000);
        }
    }

    @Override
    public void write(SeaTunnelRow value, VectorizedRowBatch output) {
        Preconditions.checkArgument(value != null, "value must not be null");
        writer.writeRow(value, output);
    }

    @Override
    public List<OrcValueWriter<?>> writers() {
        return writer.writers();
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
        return writer.metrics();
    }

    private static class RecordWriter extends GenericOrcWriters.StructWriter<SeaTunnelRow> {

        RecordWriter(List<OrcValueWriter<?>> writers) {
            super(writers);
        }

        @Override
        protected Object get(SeaTunnelRow struct, int index) {
            return struct.getField(index);
        }
    }
}
