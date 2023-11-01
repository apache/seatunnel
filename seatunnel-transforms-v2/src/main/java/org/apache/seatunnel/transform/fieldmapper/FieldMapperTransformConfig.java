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

package org.apache.seatunnel.transform.fieldmapper;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAnySetter;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class FieldMapperTransformConfig implements Serializable {
    public static final Option<Map<String, String>> FIELD_MAPPER =
            Options.key("field_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the field mapping relationship between input and output");

    public static final Option<List<TableTransforms>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableTransforms.class)
                    .noDefaultValue()
                    .withDescription("");

    @Data
    public static class TableTransforms implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        private Map<String, String> fieldMapper = new LinkedHashMap<>();

        @JsonAnySetter
        public void add(String key, String value) {
            // TODO Currently, ReadonlyConfig does not support storing objects, so special handling
            // is required
            fieldMapper.put(key.substring("fieldMapper.".length()), value);
        }
    }

    private Map<String, String> fieldMapper = new LinkedHashMap<>();

    public static FieldMapperTransformConfig of(ReadonlyConfig config) {
        FieldMapperTransformConfig fieldMapperTransformConfig = new FieldMapperTransformConfig();
        fieldMapperTransformConfig.setFieldMapper(config.get(FIELD_MAPPER));
        return fieldMapperTransformConfig;
    }

    public static FieldMapperTransformConfig of(ReadonlyConfig config, CatalogTable catalogTable) {
        String tablePath = catalogTable.getTableId().toTablePath().getFullName();
        if (null != config.get(MULTI_TABLES)) {
            return config.get(MULTI_TABLES).stream()
                    .filter(tableTransforms -> tableTransforms.getTablePath().equals(tablePath))
                    .findFirst()
                    .map(
                            tableTransforms -> {
                                FieldMapperTransformConfig fieldMapperTransformConfig =
                                        new FieldMapperTransformConfig();
                                fieldMapperTransformConfig.setFieldMapper(
                                        tableTransforms.getFieldMapper());
                                return fieldMapperTransformConfig;
                            })
                    .orElseGet(() -> of(config));
        }
        return of(config);
    }
}
