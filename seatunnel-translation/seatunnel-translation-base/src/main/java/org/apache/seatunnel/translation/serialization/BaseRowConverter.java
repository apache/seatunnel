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

package org.apache.seatunnel.translation.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Slf4j
public class BaseRowConverter extends RowConverter<SeaTunnelRow> {

    public BaseRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
    }

    @Override
    public SeaTunnelRow convert(SeaTunnelRow seaTunnelRow) throws IOException {
        validate(seaTunnelRow);
        return (SeaTunnelRow) convert(seaTunnelRow, dataType);
    }

    protected Object convert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                return convert(seaTunnelRow, rowType);
            case DATE:
                if (field instanceof LocalDate) {
                    return Date.valueOf((LocalDate) field);
                }
                break;
            case TIMESTAMP:
                if (field instanceof LocalDateTime) {
                    return Timestamp.valueOf((LocalDateTime) field);
                }
                break;
            case TIME:
                if (field instanceof LocalTime) {
                    return Time.valueOf((LocalTime) field);
                }
                break;
        }
        return field;
    }

    private SeaTunnelRow convert(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        int arity = rowType.getTotalFields();
        Object[] values = new Object[arity];
        for (int i = 0; i < arity; i++) {
            Object fieldValue = convert(seaTunnelRow.getField(i), rowType.getFieldType(i));
            if (fieldValue != null) {
                values[i] = fieldValue;
            }
        }
        return new SeaTunnelRow(values);
    }

    @Override
    public SeaTunnelRow reconvert(SeaTunnelRow engineRow) throws IOException {
        return (SeaTunnelRow) reconvert(engineRow, dataType);
    }

    protected Object reconvert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }

        switch (dataType.getSqlType()) {
            case ROW:
                return reconvert((SeaTunnelRow) field, (SeaTunnelRowType) dataType);
            case DATE:
                if (field instanceof Date) {
                    return ((Date) field).toLocalDate();
                }
                break;
            case TIMESTAMP:
                if (field instanceof Timestamp) {
                    return ((Timestamp) field).toLocalDateTime();
                }
                break;
            case TIME:
                if (field instanceof Time) {
                    return ((Time) field).toLocalTime();
                }
                break;
        }
        return field;
    }

    protected SeaTunnelRow reconvert(SeaTunnelRow engineRow, SeaTunnelRowType rowType) {
        int num = engineRow.getFields().length;
        Object[] fields = new Object[num];
        for (int i = 0; i < num; i++) {
            fields[i] = reconvert(engineRow.getFields()[i], rowType.getFieldType(i));
        }
        return new SeaTunnelRow(fields);
    }
}
