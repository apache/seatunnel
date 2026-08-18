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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.encrypt.encryptor.Encryptor;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

public class FieldEncryptTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "FieldEncrypt";

    private static final String ENCRYPT = "ENCRYPT";
    private static final String DECRYPT = "DECRYPT";

    private final List<String> fields = new ArrayList<>();
    private final String key;
    private final String encryptAlgorithm;
    private final String mode;
    private final int maxFieldLength;

    private transient volatile Encryptor encryptor;
    private int[] encryptFieldIndexes;

    public FieldEncryptTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);

        this.fields.addAll(config.get(FieldEncryptTransformConfig.FIELDS));
        this.key = config.get(FieldEncryptTransformConfig.KEY);
        this.encryptAlgorithm = config.get(FieldEncryptTransformConfig.ALGORITHM);
        this.mode = config.get(FieldEncryptTransformConfig.MODE);
        this.maxFieldLength = config.get(FieldEncryptTransformConfig.MAX_FIELD_LENGTH);

        initializeFieldIndexes();
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (encryptor == null) {
            ServiceLoader<Encryptor> loader = ServiceLoader.load(Encryptor.class);
            Optional<Encryptor> optionalEncryptor =
                    StreamSupport.stream(loader.spliterator(), false)
                            .filter(e -> e.support(encryptAlgorithm))
                            .findFirst();

            if (!optionalEncryptor.isPresent()) {
                throw CommonError.unsupportedOperation(
                        PLUGIN_NAME, "Unsupported encrypt algorithm");
            }
            this.encryptor = optionalEncryptor.get();
            this.encryptor.init(this.key);
        }

        if (ENCRYPT.equalsIgnoreCase(mode)) {
            return processFields(inputRow, encryptor::encrypt);
        } else if (DECRYPT.equalsIgnoreCase(mode)) {
            return processFields(inputRow, encryptor::decrypt);
        } else {
            throw CommonError.illegalArgument(mode, "mode only support encrypt or decrypt");
        }
    }

    private SeaTunnelRow processFields(SeaTunnelRow inputRow, UnaryOperator<String> action) {
        SeaTunnelRow outputRow = inputRow.copy();
        for (int index : encryptFieldIndexes) {
            Object field = outputRow.getField(index);
            if (field == null) {
                continue;
            }

            String value = field.toString();
            if (value.length() > maxFieldLength) {
                throw CommonError.illegalArgument(
                        String.valueOf(value.length()),
                        "Field length exceeds the maximum limit of " + maxFieldLength);
            }

            outputRow.setField(index, action.apply(value));
        }
        return outputRow;
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
                                    throw TransformCommonError.cannotFindInputFieldError(
                                            PLUGIN_NAME, fieldName);
                                })
                        .toArray();
    }
}
