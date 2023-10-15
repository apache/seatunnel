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
package org.apache.seatunnel.transform.jsonpath;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.exception.TransformException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.INPUT_FIELD_NOT_FOUND;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_COMPILE_ERROR;

@Slf4j
@AutoService(JsonPathTransform.class)
public class JsonPathTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "JsonPath";
    private JsonPathTransformConfig config;
    private SeaTunnelRowType seaTunnelRowType;

    private String[] outputFieldNames;

    private SeaTunnelDataType[] dataTypes;

    private int[] srcFieldIndexArr;

    private static final Map<String, JsonPath> JSON_PATH_CACHE = new ConcurrentHashMap<>();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonPathTransform(JsonPathTransformConfig config, CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        initOutputFields();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ReadonlyConfig fromConfig = ReadonlyConfig.fromConfig(pluginConfig);
        ConfigValidator.of(fromConfig).validate(new JsonPathTransformFactory().optionRule());
        if (config == null) {
            this.config = JsonPathTransformConfig.of(fromConfig);
        }
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
        this.seaTunnelRowType = inputRowType;
        initOutputFields();
    }

    private void initOutputFields() {

        if (outputFieldNames != null || dataTypes != null || srcFieldIndexArr != null) {
            return;
        }

        List<FiledConfig> filedConfigs = this.config.getFiledConfigs();

        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Set<String> fieldNameSet = new HashSet<>(Arrays.asList(fieldNames));
        this.outputFieldNames = new String[filedConfigs.size()];
        this.dataTypes = new SeaTunnelDataType[outputFieldNames.length];
        this.srcFieldIndexArr = new int[outputFieldNames.length];

        for (int i = 0; i < filedConfigs.size(); i++) {
            FiledConfig filedConfig = filedConfigs.get(i);
            String srcField = filedConfig.getSrcField();
            if (!fieldNameSet.contains(srcField)) {
                throw new TransformException(
                        INPUT_FIELD_NOT_FOUND,
                        String.format(
                                "JsonPathTransform config not found src_field:[%s]", srcField));
            }
            this.outputFieldNames[i] = filedConfig.getDestField();
            this.srcFieldIndexArr[i] = seaTunnelRowType.indexOf(srcField);
        }
        this.dataTypes =
                IntStream.range(0, filedConfigs.size())
                        .mapToObj((IntFunction<SeaTunnelDataType>) value -> BasicType.STRING_TYPE)
                        .toArray(value -> new SeaTunnelDataType[value]);
    }

    @Override
    protected String[] getOutputFieldNames() {
        return outputFieldNames;
    }

    @Override
    protected SeaTunnelDataType[] getOutputFieldDataTypes() {
        return this.dataTypes;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        List<FiledConfig> configs = this.config.getFiledConfigs();
        int size = configs.size();
        Object[] fieldValues = new Object[size];
        for (int i = 0; i < size; i++) {
            int pos = this.srcFieldIndexArr[i];
            fieldValues[i] =
                    doTransform(
                            seaTunnelRowType.getFieldType(pos),
                            inputRow.getField(pos),
                            configs.get(i));
        }
        return fieldValues;
    }

    private Object doTransform(SeaTunnelDataType dataType, Object value, FiledConfig filedConfig) {
        if (value == null) {
            return null;
        }
        JSON_PATH_CACHE.computeIfAbsent(filedConfig.getPath(), JsonPath::compile);
        String jsonString = "";
        try {
            switch (dataType.getSqlType()) {
                case STRING:
                    jsonString = value.toString();
                    break;
                case BYTES:
                    jsonString = new String((byte[]) value);
                    break;
                case ARRAY:
                case MAP:
                case ROW:
                case MULTIPLE_ROW:
                    jsonString = OBJECT_MAPPER.writeValueAsString(value);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "JsonPathTransform unsupported type: " + dataType.getSqlType());
            }
            Object parseResult = JSON_PATH_CACHE.get(filedConfig.getPath()).read(jsonString);
            if (parseResult instanceof String) {
                return parseResult;
            } else {
                return OBJECT_MAPPER.writeValueAsString(parseResult);
            }
        } catch (JsonProcessingException e) {
            throw new TransformException(
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format(
                            "JsonPathTransform try convert %s to json for jsonpath failed", value));
        } catch (JsonPathException e) {
            throw new TransformException(JSON_PATH_COMPILE_ERROR, e.getMessage());
        }
    }

    @Override
    protected Column[] getOutputColumns() {
        return Arrays.stream(this.outputFieldNames)
                .map(field -> PhysicalColumn.of(field, BasicType.STRING_TYPE, 200, true, "", ""))
                .toArray(Column[]::new);
    }
}
