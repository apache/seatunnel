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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.data.PaimonTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** The util seatunnel schema to paimon schema */
public class SchemaUtil {

    public static DataType toPaimonType(Column column) {
        BasicTypeDefine<DataType> basicTypeDefine = PaimonTypeMapper.INSTANCE.reconvert(column);
        return basicTypeDefine.getNativeType();
    }

    public static Schema toPaimonSchema(
            TableSchema tableSchema, PaimonSinkConfig paimonSinkConfig) {
        Schema.Builder paiSchemaBuilder = Schema.newBuilder();
        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
            Column column = tableSchema.getColumns().get(i);
            paiSchemaBuilder.column(column.getName(), toPaimonType(column));
        }
        List<String> primaryKeys = paimonSinkConfig.getPrimaryKeys();
        if (primaryKeys.isEmpty() && Objects.nonNull(tableSchema.getPrimaryKey())) {
            primaryKeys = tableSchema.getPrimaryKey().getColumnNames();
        }
        if (!primaryKeys.isEmpty()) {
            paiSchemaBuilder.primaryKey(primaryKeys);
        }
        List<String> partitionKeys = paimonSinkConfig.getPartitionKeys();
        if (!partitionKeys.isEmpty()) {
            paiSchemaBuilder.partitionKeys(partitionKeys);
        }
        Map<String, String> writeProps = paimonSinkConfig.getWriteProps();
        CoreOptions.ChangelogProducer changelogProducer = paimonSinkConfig.getChangelogProducer();
        if (changelogProducer != null) {
            writeProps.remove(PaimonSinkConfig.CHANGELOG_TMP_PATH);
        }
        if (!writeProps.isEmpty()) {
            paiSchemaBuilder.options(writeProps);
        }
        return paiSchemaBuilder.build();
    }

    public static Column toSeaTunnelType(BasicTypeDefine<DataType> typeDefine) {
        return PaimonTypeMapper.INSTANCE.convert(typeDefine);
    }

    public static DataField getDataField(List<DataField> fields, String fieldName) {
        Optional<DataField> firstField =
                fields.stream().filter(field -> field.name().equals(fieldName)).findFirst();
        if (!firstField.isPresent()) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.GET_FILED_FAILED,
                    "Can not get the filed [" + fieldName + "] from source table");
        }
        return firstField.get();
    }
}
