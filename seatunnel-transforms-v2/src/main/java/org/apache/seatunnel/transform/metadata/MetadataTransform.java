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

package org.apache.seatunnel.transform.metadata;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.MetadataUtil.isMetadataField;

public class MetadataTransform extends MultipleFieldOutputTransform {

    private List<String> fieldNames;
    private MetadataSchema metadataSchema;
    private Map<String, String> metadataFieldMapping;

    public MetadataTransform(ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        initOutputFields(inputCatalogTable, config.get(MetadataTransformConfig.METADATA_FIELDS));
    }

    private void initOutputFields(CatalogTable inputCatalogTable, Map<String, String> fields) {
        List<String> sourceTableFiledNames =
                Arrays.asList(inputCatalogTable.getTableSchema().getFieldNames());
        List<String> fieldNames = new ArrayList<>();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String srcField = field.getKey();
            if (!isMetadataField(srcField)) {
                throw TransformCommonError.cannotFindMetadataFieldError(getPluginName(), srcField);
            }
            String targetField = field.getValue();
            if (sourceTableFiledNames.contains(targetField)) {
                throw TransformCommonError.metadataMappingFieldExists(getPluginName(), srcField);
            }
            fieldNames.add(field.getKey());
        }
        this.fieldNames = fieldNames;
        this.metadataSchema = inputCatalogTable.getMetadataSchema();
        this.metadataFieldMapping = fields;
    }

    @Override
    public String getPluginName() {
        return MetadataTransformConfig.PLUGIN_NAME;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] value = new Object[fieldNames.size()];
        for (Map.Entry<String, String> mapping : metadataFieldMapping.entrySet()) {
            String metadataFieldName = mapping.getKey();
            int i = fieldNames.indexOf(metadataFieldName);
            Object fieldValue;
            switch (CommonOptions.fromName(metadataFieldName)) {
                case DATABASE:
                    fieldValue = MetadataUtil.getDatabase(inputRow);
                    break;
                case TABLE:
                    fieldValue = MetadataUtil.getTable(inputRow);
                    break;
                case ROW_KIND:
                    fieldValue = MetadataUtil.getRowKind(inputRow);
                    break;
                default:
                    fieldValue = inputRow.getOptions().get(metadataFieldName);
            }
            value[i] = fieldValue;
        }
        return value;
    }

    @Override
    protected Column[] getOutputColumns() {
        Column[] columns = new Column[fieldNames.size()];
        for (Map.Entry<String, String> mapping : metadataFieldMapping.entrySet()) {
            String metadataFieldName = mapping.getKey();
            String mappingFieldName = mapping.getValue();
            int i = fieldNames.indexOf(metadataFieldName);
            Column column;

            switch (CommonOptions.fromName(metadataFieldName)) {
                case DATABASE:
                case TABLE:
                case ROW_KIND:
                    column =
                            PhysicalColumn.of(
                                    mappingFieldName,
                                    BasicType.STRING_TYPE,
                                    (Long) null,
                                    null,
                                    true,
                                    null,
                                    null);
                    break;
                default:
                    if (metadataSchema.contains(metadataFieldName)) {
                        column =
                                ((MetadataColumn)
                                                metadataSchema
                                                        .getColumn(metadataFieldName)
                                                        .rename(mappingFieldName))
                                        .toPhysicalColumn();
                    } else {
                        throw TransformCommonError.cannotFindMetadataFieldError(
                                getPluginName(), mappingFieldName);
                    }
            }
            columns[i] = column;
        }
        return columns;
    }

    @VisibleForTesting
    public void initRowContainerGenerator() {
        transformTableSchema();
    }
}
