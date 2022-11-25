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

package org.apache.seatunnel.connectors.seatunnel.mongodb.data;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import lombok.Getter;
import lombok.SneakyThrows;
import org.bson.AbstractBsonWriter;
import org.bson.BsonBinary;
import org.bson.BsonContextType;
import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeaTunnelRowBsonWriter extends AbstractBsonWriter {

    private final SeaTunnelRowType rowType;
    private final SeaTunnelRow row;

    public SeaTunnelRowBsonWriter(SeaTunnelRowType rowType,
                                  SeaTunnelRow row) {
        super(new BsonWriterSettings());
        this.rowType = rowType;
        this.row = row;
        setContext(new SeaTunnelContext(BsonContextType.TOP_LEVEL));
    }

    @Override
    protected SeaTunnelContext getContext() {
        return (SeaTunnelContext) super.getContext();
    }

    @Override
    protected void doWriteStartDocument() {
        BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
        if (BsonContextType.TOP_LEVEL == getContext().getContextType()) {
            setContext(new SeaTunnelContext(getContext(), contextType, rowType, row));
        } else {
            SeaTunnelDataType<?> dataType = getSeaTunnelDataType();
            Object dataContainer = createDataContainer(dataType);

            setContext(new SeaTunnelContext(getContext(), contextType, dataType, dataContainer));
        }
    }

    @Override
    protected void doWriteEndDocument() {
        if (getContext().getContextType() == BsonContextType.SCOPE_DOCUMENT) {
            setContext(getContext().getParentContext());
            writeEndDocument();
        } else {
            SeaTunnelContext childContext = getContext();
            setContext(childContext.getParentContext());

            SeaTunnelContext currentContext = getContext();
            if (BsonContextType.TOP_LEVEL != currentContext.getContextType()) {
                currentContext.write(getName(), childContext.getDataContainer());
            }
        }
    }

    @Override
    protected void doWriteStartArray() {
        SeaTunnelRowType parentDataType = (SeaTunnelRowType) getContext().getDataType();
        int index = parentDataType.indexOf(getName());
        ArrayType dataType = (ArrayType) parentDataType.getFieldType(index);
        List dataContainer = (List) createDataContainer(dataType);

        setContext(new SeaTunnelContext(getContext(), BsonContextType.ARRAY, dataType, dataContainer));
    }

    @SneakyThrows
    @Override
    protected void doWriteEndArray() {
        SeaTunnelContext childContext = getContext();
        ArrayType dataType = (ArrayType) childContext.getDataType();
        setContext(childContext.getParentContext());

        List dataContainer = (List) childContext.getDataContainer();
        Object[] arrayType = (Object[]) Array.newInstance(dataType.getElementType().getTypeClass(), 0);
        getContext().write(getName(), dataContainer.toArray(arrayType));
    }

    @Override
    protected void doWriteObjectId(ObjectId value) {
        getContext().write(getName(), value.toString());
    }

    @Override
    protected void doWriteNull() {
        getContext().write(getName(), null);
    }

    @Override
    protected void doWriteUndefined() {
        getContext().write(getName(), null);
    }

    @Override
    protected void doWriteBinaryData(BsonBinary value) {
        getContext().write(getName(), value.getData());
    }

    @Override
    protected void doWriteBoolean(boolean value) {
        getContext().write(getName(), value);
    }

    @Override
    protected void doWriteDouble(double value) {
        SeaTunnelDataType<?> fieldType = getSeaTunnelDataType();
        switch (fieldType.getSqlType()) {
            case FLOAT:
                getContext().write(getName(), Double.valueOf(value).floatValue());
                break;
            case DOUBLE:
            default:
                getContext().write(getName(), value);
        }
    }

    @Override
    protected void doWriteInt32(int value) {
        SeaTunnelDataType<?> fieldType = getSeaTunnelDataType();
        switch (fieldType.getSqlType()) {
            case TINYINT:
                getContext().write(getName(), Integer.valueOf(value).byteValue());
                break;
            case SMALLINT:
                getContext().write(getName(), Integer.valueOf(value).shortValue());
                break;
            case INT:
            default:
                getContext().write(getName(), value);
        }
    }

    @Override
    protected void doWriteInt64(long value) {
        getContext().write(getName(), value);
    }

    @Override
    protected void doWriteDecimal128(Decimal128 value) {
        getContext().write(getName(), value.bigDecimalValue());
    }

    @Override
    protected void doWriteJavaScript(String value) {
        getContext().write(getName(), value);
    }

    @Override
    protected void doWriteString(String value) {
        getContext().write(getName(), value);
    }

    @Override
    protected void doWriteSymbol(String value) {
        getContext().write(getName(), value);
    }

    @Override
    protected void doWriteDateTime(long value) {
        SeaTunnelDataType<?> fieldType = getSeaTunnelDataType();
        switch (fieldType.getSqlType()) {
            case DATE:
                LocalDate localDate = new Date(value)
                    .toInstant()
                    .atZone(ZoneOffset.UTC)
                    .toLocalDate();
                getContext().write(getName(), localDate);
                break;
            case TIMESTAMP:
            default:
                LocalDateTime localDateTime = new Date(value)
                    .toInstant()
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime();
                getContext().write(getName(), localDateTime);
        }
    }

    @Override
    protected void doWriteTimestamp(BsonTimestamp value) {
        LocalDateTime localDateTime = new Date(value.getValue())
            .toInstant()
            .atZone(ZoneOffset.UTC)
            .toLocalDateTime();
        getContext().write(getName(), localDateTime);
    }

    @Override
    protected void doWriteJavaScriptWithScope(String value) {
        throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported JavaScriptWithScope");
    }

    @Override
    protected void doWriteMaxKey() {
        throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported MaxKey");
    }

    @Override
    protected void doWriteMinKey() {
        throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported MinKey");
    }

    @Override
    protected void doWriteRegularExpression(BsonRegularExpression value) {
        throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported BsonRegularExpression");
    }

    @Override
    protected void doWriteDBPointer(BsonDbPointer value) {
        throw new MongodbConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported BsonDbPointer");
    }

    @Override
    public void flush() {
    }

    private SeaTunnelDataType getSeaTunnelDataType() {
        SeaTunnelDataType dataType = getContext().getDataType();
        switch (dataType.getSqlType()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                return arrayType.getElementType();
            case MAP:
                MapType mapType = (MapType) dataType;
                return mapType.getValueType();
            case ROW:
            default:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                int index = rowType.indexOf(getName());
                return rowType.getFieldType(index);
        }
    }

    private static Object createDataContainer(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case ARRAY:
                return new ArrayList<>();
            case MAP:
                return new HashMap<>();
            case ROW:
            default:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                return new SeaTunnelRow(rowType.getTotalFields());
        }
    }

    @Getter
    private class SeaTunnelContext extends Context {
        private final SeaTunnelDataType dataType;
        private final Object dataContainer;

        public SeaTunnelContext(final BsonContextType contextType) {
            this(null, contextType, null, null);
        }

        public SeaTunnelContext(final SeaTunnelContext parentContext,
                                final BsonContextType contextType,
                                final SeaTunnelDataType dataType,
                                final Object dataContainer) {
            super(parentContext, contextType);
            this.dataType = dataType;
            this.dataContainer = dataContainer;
        }

        @Override
        public SeaTunnelContext getParentContext() {
            return (SeaTunnelContext) super.getParentContext();
        }

        public void write(String fieldName, Object value) {
            switch (dataType.getSqlType()) {
                case ARRAY:
                    ((List) dataContainer).add(value);
                    break;
                case MAP:
                    ((Map) dataContainer).put(fieldName, value);
                    break;
                case ROW:
                default:
                    SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                    SeaTunnelRow row = (SeaTunnelRow) dataContainer;
                    int index = rowType.indexOf(fieldName);
                    row.setField(index, value);
                    break;
            }
        }
    }
}
