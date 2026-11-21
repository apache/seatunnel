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

/**
 * Projects logical row metadata into physical columns.
 *
 * <p>A {@code metadata_fields} key is accepted when it is globally registered in {@link
 * MetadataUtil} or explicitly declared by the input table's {@link MetadataSchema}. Keys that exist
 * only in {@code SeaTunnelRow.options} are rejected so that {@link MetadataSchema} remains the type
 * and trust boundary for connector-specific fields.
 */
public class MetadataTransform extends MultipleFieldOutputTransform {

    private List<String> fieldNames;
    private MetadataSchema metadataSchema;
    private Map<String, String> metadataFieldMapping;

    public MetadataTransform(ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        initOutputFields(inputCatalogTable, config.get(MetadataTransformConfig.METADATA_FIELDS));
    }

    /**
     * Validates {@code metadata_fields} mappings and records the projection order.
     *
     * <p>Connector-specific keys must be declared in the input {@link MetadataSchema}. Matching is
     * case-sensitive. Duplicate physical output names are still rejected.
     *
     * @param inputCatalogTable upstream catalog table that supplies the metadata schema
     * @param fields mapping from logical metadata key to physical output name
     */
    private void initOutputFields(CatalogTable inputCatalogTable, Map<String, String> fields) {
        List<String> sourceTableFiledNames =
                Arrays.asList(inputCatalogTable.getTableSchema().getFieldNames());
        this.metadataSchema = inputCatalogTable.getMetadataSchema();
        List<String> fieldNames = new ArrayList<>();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String srcField = field.getKey();
            if (!isProjectableMetadataField(srcField)) {
                throw TransformCommonError.cannotFindMetadataFieldError(getPluginName(), srcField);
            }
            String targetField = field.getValue();
            if (sourceTableFiledNames.contains(targetField)) {
                throw TransformCommonError.metadataMappingFieldExists(getPluginName(), srcField);
            }
            fieldNames.add(field.getKey());
        }
        this.fieldNames = fieldNames;
        this.metadataFieldMapping = fields;
    }

    /**
     * Returns whether {@code fieldName} can be projected by this transform.
     *
     * <p>Globally registered keys keep their existing behavior. Connector-specific keys are allowed
     * only when the input {@link MetadataSchema} declares them. Physical columns that happen to use
     * the same name are not treated as metadata.
     *
     * @param fieldName logical metadata key from {@code metadata_fields}
     * @return {@code true} if the key is globally registered or schema-declared
     */
    private boolean isProjectableMetadataField(String fieldName) {
        if (isMetadataField(fieldName)) {
            return true;
        }
        return metadataSchema != null && metadataSchema.contains(fieldName);
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
            value[i] = getMetadataFieldValue(metadataFieldName, inputRow);
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

            if (isComputedCommonMetadataField(metadataFieldName)) {
                column =
                        PhysicalColumn.of(
                                mappingFieldName,
                                BasicType.STRING_TYPE,
                                (Long) null,
                                null,
                                true,
                                null,
                                null);
            } else if (metadataSchema != null && metadataSchema.contains(metadataFieldName)) {
                column =
                        ((MetadataColumn)
                                        metadataSchema
                                                .getColumn(metadataFieldName)
                                                .rename(mappingFieldName))
                                .toPhysicalColumn();
            } else {
                throw TransformCommonError.cannotFindMetadataFieldError(
                        getPluginName(), metadataFieldName);
            }
            columns[i] = column;
        }
        return columns;
    }

    private Object getMetadataFieldValue(String metadataFieldName, SeaTunnelRowAccessor inputRow) {
        if (CommonOptions.DATABASE.getName().equals(metadataFieldName)) {
            return MetadataUtil.getDatabase(inputRow);
        }
        if (CommonOptions.TABLE.getName().equals(metadataFieldName)) {
            return MetadataUtil.getTable(inputRow);
        }
        if (CommonOptions.ROW_KIND.getName().equals(metadataFieldName)) {
            return MetadataUtil.getRowKind(inputRow);
        }
        return inputRow.getOptions().get(metadataFieldName);
    }

    private boolean isComputedCommonMetadataField(String metadataFieldName) {
        return CommonOptions.DATABASE.getName().equals(metadataFieldName)
                || CommonOptions.TABLE.getName().equals(metadataFieldName)
                || CommonOptions.ROW_KIND.getName().equals(metadataFieldName);
    }

    @VisibleForTesting
    public void initRowContainerGenerator() {
        transformTableSchema();
    }
}
