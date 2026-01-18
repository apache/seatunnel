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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public class FieldEncryptTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "FieldEncrypt";

    private static final String ENCRYPT = "ENCRYPT";
    private static final String DECRYPT = "DECRYPT";

    private final List<String> fields = new ArrayList<>();
    private final String key;
    private final Encryptor encryptor;
    private final String mode;

    private int[] encryptFieldIndexes;

    public FieldEncryptTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);

        this.fields.addAll(config.get(FieldEncryptTransformConfig.FIELDS));
        this.key = config.get(FieldEncryptTransformConfig.KEY);
        this.mode = config.get(FieldEncryptTransformConfig.MODE);

        EncryptAlgorithm encryptAlgorithm = config.get(FieldEncryptTransformConfig.ALGORITHM);
        switch (encryptAlgorithm) {
                // TODO: support more algorithms
            case AES_CBC:
                this.encryptor = new AesCbcEncryptor(key);
                break;
            default:
                throw CommonError.unsupportedOperation(
                        PLUGIN_NAME, "Unsupported encrypt algorithm");
        }

        initializeFieldIndexes();
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (ENCRYPT.equalsIgnoreCase(mode)) {
            for (int index : encryptFieldIndexes) {
                Object field = inputRow.getField(index);
                if (field == null) {
                    continue;
                }
                String value = field.toString();
                if (StringUtils.isNotBlank(value)) {
                    inputRow.setField(index, encryptor.encrypt(value));
                }
            }
            return inputRow;
        } else if (DECRYPT.equalsIgnoreCase(mode)) {
            for (int index : encryptFieldIndexes) {
                Object field = inputRow.getField(index);
                if (field == null) {
                    continue;
                }
                String value = field.toString();
                if (StringUtils.isNotBlank(value)) {
                    inputRow.setField(index, encryptor.decrypt(value));
                }
            }
            return inputRow;
        } else {
            throw CommonError.illegalArgument(mode, "mode only support encrypt or decrypt");
        }
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    private void initializeFieldIndexes() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        encryptFieldIndexes =
                fields.stream()
                        .mapToInt(
                                fieldName -> {
                                    for (int i = 0; i < columns.size(); i++) {
                                        if (columns.get(i).getName().equals(fieldName)) {
                                            if (BasicType.STRING_TYPE.equals(
                                                    columns.get(i).getDataType())) {
                                                return i;
                                            } else {
                                                throw CommonError.unsupportedDataType(
                                                        PLUGIN_NAME,
                                                        columns.get(i).getDataType().toString(),
                                                        columns.get(i).getName());
                                            }
                                        }
                                    }
                                    throw new IllegalArgumentException(
                                            "Field not found: " + fieldName);
                                })
                        .toArray();
    }
}
