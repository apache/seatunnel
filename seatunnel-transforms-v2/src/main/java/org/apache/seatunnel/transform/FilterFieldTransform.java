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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class FilterFieldTransform extends AbstractSeaTunnelTransform {
    public static final Option<List<String>> KEY_FIELDS = Options.key("fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields you want to filter");

    private String[] fields;
    private int[] inputValueIndex;

    @Override
    public String getPluginName() {
        return "Filter";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        if (!pluginConfig.hasPath(KEY_FIELDS.key())) {
            throw new IllegalArgumentException("The configuration missing key: " + KEY_FIELDS);
        }
        this.fields = pluginConfig.getStringList(KEY_FIELDS.key()).toArray(new String[0]);
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        int[] inputValueIndex = new int[fields.length];
        SeaTunnelDataType[] fieldDataTypes = new SeaTunnelDataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            int inputFieldIndex = inputRowType.indexOf(field);
            if (inputFieldIndex == -1) {
                throw new IllegalArgumentException("Cannot find [" + field + "] field in input row type");
            }

            fieldDataTypes[i] = inputRowType.getFieldType(inputFieldIndex);
            inputValueIndex[i] = inputFieldIndex;
        }
        SeaTunnelRowType outputRowType = new SeaTunnelRowType(fields, fieldDataTypes);
        log.info("Changed input row type: {} to output row type: {}", inputRowType, outputRowType);

        this.inputValueIndex = inputValueIndex;
        return outputRowType;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // todo reuse array container if not remove fields
        Object[] values = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            values[i] = inputRow.getField(inputValueIndex[i]);
        }
        return new SeaTunnelRow(values);
    }
}
