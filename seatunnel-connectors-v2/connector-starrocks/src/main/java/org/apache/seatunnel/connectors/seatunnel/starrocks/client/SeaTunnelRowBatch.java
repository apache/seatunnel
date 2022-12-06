// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.starrocks.thrift.TScanBatchResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SeaTunnelRowBatch {

    // offset for iterate the rowBatch
    private SeaTunnelDataType<?>[] seaTunnelDataTypes;
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<SeaTunnelRow> seaTunnelRowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public SeaTunnelRowBatch(TScanBatchResult nextResult, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelDataTypes = seaTunnelRowType.getFieldTypes();
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(
                new ByteArrayInputStream(nextResult.getRows()),
                rootAllocator
        );
        this.offsetInRowBatch = 0;
    }

    public SeaTunnelRowBatch readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() != seaTunnelDataTypes.length) {
                    log.error("seaTunnel Schema size '{}' is not equal to arrow field size '{}'.",
                            fieldVectors.size(), seaTunnelDataTypes.length);
                    throw new RuntimeException("Load StarRocks data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    log.debug("One batch in arrow has no data.");
                    continue;
                }
                log.info("get getRowCount {}", root.getRowCount());
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    seaTunnelRowBatch.add(new SeaTunnelRow(fieldVectors.size()));
                }

                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
            return this;
        } catch (Exception e) {
            log.error("Read StarRocks Data failed because: ", e);
            throw new RuntimeException(e.getMessage());
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }

    private void addValueToRow(int rowIndex, int colIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
                    rowCountInOneBatch;
            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        seaTunnelRowBatch.get(readRowCount + rowIndex).setField(colIndex, obj);
    }

    public void convertArrowToRowBatch()  {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                SeaTunnelDataType<?> dataType = seaTunnelDataTypes[col];
                final String currentType = dataType.getSqlType().name();

                FieldVector curFieldVector = fieldVectors.get(col);
                Types.MinorType mt = curFieldVector.getMinorType();
                switch (dataType.getSqlType()) {
                    case BOOLEAN:
                        checkArgument(mt.equals(Types.MinorType.BIT),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case TINYINT:
                        checkArgument(mt.equals(Types.MinorType.TINYINT),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case SMALLINT:
                        checkArgument(mt.equals(Types.MinorType.SMALLINT),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case INT:
                        checkArgument(mt.equals(Types.MinorType.INT),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case BIGINT:

                        checkArgument(mt.equals(Types.MinorType.BIGINT),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case FLOAT:
                        checkArgument(mt.equals(Types.MinorType.FLOAT4),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case DOUBLE:
                        checkArgument(mt.equals(Types.MinorType.FLOAT8),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, col, fieldValue);
                        }
                        break;
                    case DECIMAL:
                        checkArgument(mt.equals(Types.MinorType.DECIMAL),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        DecimalVector decimalVector = (DecimalVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (decimalVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, col, null);
                                continue;
                            }
                            BigDecimal value = decimalVector.getObject(rowIndex);
                            addValueToRow(rowIndex, col, value);
                        }
                        break;
                    case DATE:
                        checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        VarCharVector date = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (date.isNull(rowIndex)) {
                                addValueToRow(rowIndex, col, null);
                                continue;
                            }
                            String value = new String(date.get(rowIndex));
                            LocalDate localDate = LocalDate.parse(value, dateFormatter);
                            addValueToRow(rowIndex, col, localDate);
                        }
                        break;
                    case TIMESTAMP:
                        checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        VarCharVector timeStampSecVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (timeStampSecVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, col, null);
                                continue;
                            }
                            String value = new String(timeStampSecVector.get(rowIndex));
                            LocalDateTime parse = LocalDateTime.parse(value, dateTimeFormatter);
                            addValueToRow(rowIndex, col, parse);
                        }
                        break;
                    case STRING:
                        checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                "seaTunnel type is %1$s, but arrow type is %2$s.", currentType, mt.name());
                        VarCharVector varCharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, col, null);
                                continue;
                            }
                            String value = new String(varCharVector.get(rowIndex));
                            addValueToRow(rowIndex, col, value);
                        }
                        break;
                    default:
                        String errMsg = "Unsupported type " + seaTunnelDataTypes[col].getSqlType().name();
                        log.error(errMsg);
                        throw new RuntimeException(errMsg);
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public SeaTunnelRow next() {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        return seaTunnelRowBatch.get(offsetInRowBatch++);
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
