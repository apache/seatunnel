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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Data
@AllArgsConstructor
public class DefineSinkTypeTransformConfig implements Serializable {

    public static final String PLUGIN_NAME = "DefineSinkType";

    public static final Option<List<DefineColumnType>> COLUMNS =
            Options.key("columns")
                    .type(new TypeReference<List<DefineColumnType>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "The columns to be defined, the name and type of the column must be set");

    public static final Option<List<TableTransforms>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableTransforms.class)
                    .noDefaultValue()
                    .withDescription("The table transform config");

    private List<DefineColumnType> columns;

    public Map<String, DefineColumnType> toMap() {
        return columns.stream()
                .collect(
                        Collectors.toMap(
                                DefineColumnType::getColumn, defineColumnType -> defineColumnType));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DefineColumnType implements Serializable {
        private String column;
        private String type;
    }

    @Data
    public static class TableTransforms implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        @JsonAlias("columns")
        private List<DefineColumnType> columns;
    }

    public static DefineSinkTypeTransformConfig of(ReadonlyConfig config) {
        List<DefineColumnType> columns = config.get(COLUMNS);

        checkArgument(columns != null && !columns.isEmpty(), "The columns must be set");
        columns.forEach(
                defineColumnType -> {
                    checkArgument(
                            defineColumnType.getColumn() != null, "The column name must be set");
                    checkArgument(
                            defineColumnType.getType() != null, "The column type must be set");
                });

        return new DefineSinkTypeTransformConfig(columns);
    }
}
