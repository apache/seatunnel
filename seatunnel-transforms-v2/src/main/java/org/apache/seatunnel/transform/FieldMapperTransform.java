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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;
import org.apache.seatunnel.transform.exception.FieldMapperTransformErrorCode;
import org.apache.seatunnel.transform.exception.FieldMapperTransformException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class FieldMapperTransform extends AbstractSeaTunnelTransform {
    public static final Option<Map<String, String>> FIELD_MAPPER = Options.key("field_mapper")
        .mapType()
        .noDefaultValue()
        .withDescription("Specify the field mapping relationship between input and output");

    private LinkedHashMap<String, String> fieldMapper = new LinkedHashMap<>();

    private List<Integer> needReaderColIndex;

    @Override
    public String getPluginName() {
        return "FieldMapper";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        if (!pluginConfig.hasPath(FIELD_MAPPER.key())) {
            throw new FieldMapperTransformException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, "The configuration missing key: " + FIELD_MAPPER);
        }
        this.fieldMapper = convertConfigToSortedMap(pluginConfig.getConfig(FIELD_MAPPER.key()));
    }

    private static LinkedHashMap<String, String> convertConfigToSortedMap(Config config) {
        // Because the entrySet in typesafe config couldn't keep key-value order
        // So use jackson parsing schema information into a map to keep key-value order
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        String json = config.root().render(options);
        ObjectNode jsonNodes = JsonUtils.parseObject(json);
        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();
        jsonNodes.fields().forEachRemaining(field -> {
            String key = field.getKey();
            JsonNode value = field.getValue();

            if (value.isTextual()) {
                fieldsMap.put(key, value.textValue());
            } else {
                String errorMsg = String.format("The value [%s] of key [%s] that in config is not text", value, key);
                throw new FieldMapperTransformException(CommonErrorCode.ILLEGAL_ARGUMENT, errorMsg);
            }
        });
        return fieldsMap;
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        needReaderColIndex = new ArrayList<>(fieldMapper.size());
        List<String> outputFiledNameList = new ArrayList<>(fieldMapper.size());
        List<SeaTunnelDataType<?>> outputDataTypeList = new ArrayList<>(fieldMapper.size());
        ArrayList<String> inputFieldNames = Lists.newArrayList(inputRowType.getFieldNames());
        fieldMapper.forEach((key, value) -> {
            int fieldIndex = inputFieldNames.indexOf(key);
            if (fieldIndex < 0) {
                throw new FieldMapperTransformException(FieldMapperTransformErrorCode.INPUT_FIELD_NOT_FOUND,
                        "Can not found field " + key + " from inputRowType");
            }
            needReaderColIndex.add(fieldIndex);
            outputFiledNameList.add(value);
            outputDataTypeList.add(inputRowType.getFieldTypes()[fieldIndex]);
        });

        return new SeaTunnelRowType(outputFiledNameList.toArray(new String[0]),
            outputDataTypeList.toArray(new SeaTunnelDataType[0]));
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] outputDataArray = new Object[fieldMapper.size()];
        for (int i = 0; i < outputDataArray.length; i++) {
            outputDataArray[i] = inputRow.getField(needReaderColIndex.get(i));
        }
        return new SeaTunnelRow(outputDataArray);
    }
}
