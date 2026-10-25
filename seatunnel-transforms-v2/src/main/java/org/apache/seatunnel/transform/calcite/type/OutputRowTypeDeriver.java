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

package org.apache.seatunnel.transform.calcite.type;

import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlCall;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlIdentifier;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlKind;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlNode;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlNodeList;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlSelect;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.List;

/** Derives the SeaTunnel output row type for a validated SQL statement. */
public final class OutputRowTypeDeriver {

    private final SeaTunnelRowType inputRowType;

    public OutputRowTypeDeriver(SeaTunnelRowType inputRowType) {
        this.inputRowType = inputRowType;
    }

    /**
     * Builds the output row type using both the validated row type from Calcite and the parsed
     * select list. The select list is used as a fallback when Calcite's type collapses lossier
     * representations such as {@code VARBINARY}.
     */
    public SeaTunnelRowType derive(SqlNode validated, RelDataType validatedRowType) {
        List<RelDataTypeField> fields = validatedRowType.getFieldList();
        String[] names = new String[fields.size()];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[fields.size()];
        SqlSelect select = validated instanceof SqlSelect ? (SqlSelect) validated : null;
        SqlNodeList selectList = select == null ? null : select.getSelectList();
        boolean aligned = selectList != null && selectList.size() == fields.size();
        for (int i = 0; i < fields.size(); i++) {
            names[i] = fields.get(i).getName();
            SeaTunnelDataType<?> defaultType =
                    CalciteTypeConverter.toSeaTunnelType(fields.get(i).getType());
            if (aligned) {
                types[i] = inferProjectedType(selectList.get(i), defaultType);
            } else {
                SeaTunnelDataType<?> inputFieldType = findInputFieldType(names[i]);
                types[i] = inputFieldType != null ? inputFieldType : defaultType;
            }
        }
        return new SeaTunnelRowType(names, types);
    }

    private SeaTunnelDataType<?> inferProjectedType(
            SqlNode selectItem, SeaTunnelDataType<?> defaultType) {
        SqlNode node = unwrapAlias(selectItem);
        if (node instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            if (identifier.isStar() && inputRowType.getTotalFields() == 1) {
                return inputRowType.getFieldType(0);
            }
            SeaTunnelDataType<?> inputFieldType =
                    findInputFieldType(identifier.names.get(identifier.names.size() - 1));
            return inputFieldType != null ? inputFieldType : defaultType;
        }
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            if (isVectorReturningFunction(call) && !call.getOperandList().isEmpty()) {
                SeaTunnelDataType<?> operandType =
                        inferProjectedType(call.getOperandList().get(0), defaultType);
                if (isVectorType(operandType)) {
                    return operandType;
                }
            }
        }
        return defaultType;
    }

    private SqlNode unwrapAlias(SqlNode node) {
        if (node.getKind() == SqlKind.AS && node instanceof SqlCall) {
            return ((SqlCall) node).operand(0);
        }
        return node;
    }

    private SeaTunnelDataType<?> findInputFieldType(String fieldName) {
        for (int i = 0; i < inputRowType.getTotalFields(); i++) {
            if (inputRowType.getFieldName(i).equalsIgnoreCase(fieldName)) {
                return inputRowType.getFieldType(i);
            }
        }
        return null;
    }

    private static boolean isVectorReturningFunction(SqlCall call) {
        String functionName = call.getOperator().getName();
        return "VECTOR_NORMALIZE".equalsIgnoreCase(functionName)
                || "VECTOR_REDUCE".equalsIgnoreCase(functionName);
    }

    private static boolean isVectorType(SeaTunnelDataType<?> type) {
        if (type == null) {
            return false;
        }
        switch (type.getSqlType()) {
            case BINARY_VECTOR:
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
            case SPARSE_FLOAT_VECTOR:
                return true;
            default:
                return false;
        }
    }
}
