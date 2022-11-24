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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

@AutoService(SeaTunnelTransform.class)
public class CopyFieldTransform extends SingleFieldOutputTransform {

    public static final Option<String> SRC_FIELD = Options.key("src_field")
            .stringType()
            .noDefaultValue()
            .withDescription("Src field you want to copy");

    public static final Option<String> DEST_FIELD = Options.key("dest_field")
            .stringType()
            .noDefaultValue()
            .withDescription("Copy Src field to Dest field");

    private String srcField;
    private int srcFieldIndex;
    private SeaTunnelDataType srcFieldDataType;
    private String destField;

    @Override
    public String getPluginName() {
        return "Copy";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig,
            SRC_FIELD.key(), DEST_FIELD.key());
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }

        this.srcField = pluginConfig.getString(SRC_FIELD.key());
        this.destField = pluginConfig.getString(DEST_FIELD.key());
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
        srcFieldIndex = inputRowType.indexOf(srcField);
        if (srcFieldIndex == -1) {
            throw new IllegalArgumentException("Cannot find [" + srcField + "] field in input row type");
        }
        srcFieldDataType = inputRowType.getFieldType(srcFieldIndex);
    }

    @Override
    protected String getOutputFieldName() {
        return destField;
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return srcFieldDataType;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        return clone(srcFieldDataType, inputRow.getField(srcFieldIndex));
    }

    private Object clone(SeaTunnelDataType dataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case BOOLEAN:
            case STRING:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return value;
            case BYTES:
                byte[] bytes = (byte[]) value;
                if (bytes == null) {
                    return null;
                }
                byte[] newBytes = new byte[bytes.length];
                System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                return newBytes;
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Object[] array = (Object[]) value;
                if (array == null) {
                    return null;
                }
                Object newArray = Array.newInstance(arrayType.getElementType().getTypeClass(), array.length);
                for (int i = 0; i < array.length; i++) {
                    Array.set(newArray, i, clone(arrayType.getElementType(), array[i]));
                }
                return newArray;
            case MAP:
                MapType mapType = (MapType) dataType;
                Map map = (Map) value;
                Map newMap = new HashMap();
                for (Object key : map.keySet()) {
                    newMap.put(clone(mapType.getKeyType(), key), clone(mapType.getValueType(), map.get(key)));
                }
                return newMap;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                SeaTunnelRow row = (SeaTunnelRow) value;
                if (row == null) {
                    return null;
                }

                Object[] newFields = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    newFields[i] = clone(rowType.getFieldType(i), row.getField(i));
                }
                SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                newRow.setRowKind(row.getRowKind());
                newRow.setTableId(row.getTableId());
                return newRow;
            case NULL:
                return null;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType.getSqlType());
        }
    }
}
