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

package org.apache.seatunnel.transform.filterrowkind;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Getter
@Setter
public class FilterRowKinkTransformConfig implements Serializable {

    public static final Option<List<RowKind>> INCLUDE_KINDS =
            Options.key("include_kinds")
                    .listType(RowKind.class)
                    .noDefaultValue()
                    .withDescription("the row kinds to include");
    public static final Option<List<RowKind>> EXCLUDE_KINDS =
            Options.key("exclude_kinds")
                    .listType(RowKind.class)
                    .noDefaultValue()
                    .withDescription("the row kinds to exclude");

    public static final Option<List<TableTransforms>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableTransforms.class)
                    .noDefaultValue()
                    .withDescription("");

    @Data
    public static class TableTransforms implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        @JsonAlias("include_kinds")
        private RowKind[] includeKinds = new RowKind[] {};

        @JsonAlias("exclude_kinds")
        private RowKind[] excludeKinds = new RowKind[] {};
    }

    private Set<RowKind> includeKinds = Collections.emptySet();
    private Set<RowKind> excludeKinds = Collections.emptySet();

    public static FilterRowKinkTransformConfig of(ReadonlyConfig config) {
        FilterRowKinkTransformConfig filterRowKinkTransformConfig =
                new FilterRowKinkTransformConfig();
        if (config.get(INCLUDE_KINDS) != null) {
            filterRowKinkTransformConfig.setIncludeKinds(new HashSet<>(config.get(INCLUDE_KINDS)));
        }
        if (config.get(EXCLUDE_KINDS) != null) {
            filterRowKinkTransformConfig.setExcludeKinds(new HashSet<>(config.get(EXCLUDE_KINDS)));
        }
        return filterRowKinkTransformConfig;
    }

    public static FilterRowKinkTransformConfig of(
            ReadonlyConfig config, CatalogTable catalogTable) {
        String tablePath = catalogTable.getTableId().toTablePath().getFullName();
        if (null != config.get(MULTI_TABLES)) {
            return config.get(MULTI_TABLES).stream()
                    .filter(tableTransforms -> tableTransforms.getTablePath().equals(tablePath))
                    .findFirst()
                    .map(
                            tableTransforms -> {
                                FilterRowKinkTransformConfig filterRowKinkTransformConfig =
                                        new FilterRowKinkTransformConfig();
                                filterRowKinkTransformConfig.setIncludeKinds(
                                        new HashSet<>(
                                                Arrays.asList(tableTransforms.getIncludeKinds())));
                                filterRowKinkTransformConfig.setExcludeKinds(
                                        new HashSet<>(
                                                Arrays.asList(tableTransforms.getExcludeKinds())));
                                return filterRowKinkTransformConfig;
                            })
                    .orElseGet(() -> of(config));
        }
        return of(config);
    }
}
