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

package org.apache.seatunnel.transform.copy;

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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@Setter
public class CopyTransformConfig implements Serializable {
    @Deprecated
    public static final Option<String> SRC_FIELD =
            Options.key("src_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Src field you want to copy");

    @Deprecated
    public static final Option<String> DEST_FIELD =
            Options.key("dest_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Copy Src field to Dest field");

    public static final Option<Map<String, String>> FIELDS =
            Options.key("fields")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the field copy relationship between input and output");

    public static final Option<List<TableTransforms>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableTransforms.class)
                    .noDefaultValue()
                    .withDescription("");

    @Data
    public static class TableTransforms implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        private Map<String, String> fields = new HashMap<>();

        @JsonAnySetter
        public void add(String key, String value) {
            // TODO Currently, ReadonlyConfig does not support storing objects, so special handling
            // is required
            fields.put(key.substring("fields.".length()), value);
        }
    }

    private LinkedHashMap<String, String> fields;

    public static CopyTransformConfig of(ReadonlyConfig config) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        Optional<Map<String, String>> optional = config.getOptional(FIELDS);
        if (optional.isPresent()) {
            fields.putAll(config.get(FIELDS));
        } else {
            fields.put(config.get(DEST_FIELD), config.get(SRC_FIELD));
        }

        CopyTransformConfig copyTransformConfig = new CopyTransformConfig();
        copyTransformConfig.setFields(fields);
        return copyTransformConfig;
    }

    public static CopyTransformConfig of(ReadonlyConfig config, CatalogTable catalogTable) {

        String tableID = catalogTable.getTableId().toTablePath().toString();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        Optional<List<TableTransforms>> multiTableOp = config.getOptional(MULTI_TABLES);
        Optional<Map<String, String>> fieldsOp = config.getOptional(FIELDS);
        Optional<String> destOp = config.getOptional(DEST_FIELD);
        Optional<String> srcOp = config.getOptional(SRC_FIELD);

        if (multiTableOp.isPresent()) {
            List<TableTransforms> tableTransforms = config.get(MULTI_TABLES);
            for (TableTransforms tableTransform : tableTransforms) {
                if (tableTransform.getTablePath().equals(tableID)) {
                    fields.putAll(tableTransform.getFields());
                    break;
                }
            }
        }

        if (fields.isEmpty()) {
            if (fieldsOp.isPresent()) {
                fields.putAll(config.get(FIELDS));
            } else if (destOp.isPresent() && srcOp.isPresent()) {
                fields.put(config.get(DEST_FIELD), config.get(SRC_FIELD));
            }
        }

        CopyTransformConfig copyTransformConfig = new CopyTransformConfig();
        copyTransformConfig.setFields(fields);
        return copyTransformConfig;
    }
}
