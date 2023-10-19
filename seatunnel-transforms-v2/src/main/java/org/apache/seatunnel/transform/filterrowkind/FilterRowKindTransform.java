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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.common.FilterRowTransform;

import lombok.NonNull;
import lombok.ToString;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@ToString(of = {"includeKinds", "excludeKinds"})
public class FilterRowKindTransform extends FilterRowTransform {
    public static String PLUGIN_NAME = "FilterRowKind";

    private Set<RowKind> includeKinds = Collections.emptySet();
    private Set<RowKind> excludeKinds = Collections.emptySet();

    public FilterRowKindTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        initConfig(config);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private void initConfig(ReadonlyConfig config) {
        if (config.get(FilterRowKinkTransformConfig.INCLUDE_KINDS) == null) {
            excludeKinds =
                    new HashSet<RowKind>(config.get(FilterRowKinkTransformConfig.EXCLUDE_KINDS));
        } else {
            includeKinds =
                    new HashSet<RowKind>(config.get(FilterRowKinkTransformConfig.INCLUDE_KINDS));
        }
        if ((includeKinds.isEmpty() && excludeKinds.isEmpty())
                || (!includeKinds.isEmpty() && !excludeKinds.isEmpty())) {
            throw new SeaTunnelRuntimeException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format(
                            "These options(%s,%s) are mutually exclusive, allowing only one set of options to be configured.",
                            FilterRowKinkTransformConfig.INCLUDE_KINDS.key(),
                            FilterRowKinkTransformConfig.EXCLUDE_KINDS.key()));
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (!this.excludeKinds.isEmpty()) {
            return this.excludeKinds.contains(inputRow.getRowKind()) ? null : inputRow;
        }
        if (!this.includeKinds.isEmpty()) {
            Set<RowKind> includeKinds = this.includeKinds;
            return includeKinds.contains(inputRow.getRowKind()) ? inputRow : null;
        }
        throw new SeaTunnelRuntimeException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "Transform config error! Either excludeKinds or includeKinds must be configured");
    }
}
