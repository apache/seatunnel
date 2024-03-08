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

package org.apache.seatunnel.connectors.seatunnel.paimon.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.paimon.data.PaimonTypeMapper;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;

import java.util.Objects;

/** The util seatunnel schema to paimon schema */
public class SchemaUtil {

    public static DataType toPaimonType(Column column) {
        return PaimonTypeMapper.INSTANCE.reconvert(column);
    }

    public static Schema toPaimonSchema(TableSchema tableSchema) {
        Schema.Builder paiSchemaBuilder = Schema.newBuilder();
        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
            Column column = tableSchema.getColumns().get(i);
            paiSchemaBuilder.column(column.getName(), toPaimonType(column));
        }
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        if (Objects.nonNull(primaryKey) && primaryKey.getColumnNames().size() > 0) {
            paiSchemaBuilder.primaryKey(primaryKey.getColumnNames());
        }
        return paiSchemaBuilder.build();
    }

    public static SeaTunnelDataType<?> toSeaTunnelType(DataType dataType) {
        return PaimonTypeMapper.INSTANCE.convert(dataType).getDataType();
    }
}
