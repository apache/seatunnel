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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.common.FilterRowTransform;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import lombok.ToString;

@ToString(of = {"includeKinds", "excludeKinds"})
@AutoService(SeaTunnelTransform.class)
public class FilterRowKindTransform extends FilterRowTransform {
    private FilterRowKinkTransformConfig config;

    public FilterRowKindTransform() {
        super();
    }

    public FilterRowKindTransform(
            @NonNull FilterRowKinkTransformConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "FilterRowKind";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new FilterRowKindTransformFactory().optionRule());
        this.config = FilterRowKinkTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (!this.config.getExcludeKinds().isEmpty()) {
            return this.config.getExcludeKinds().contains(inputRow.getRowKind()) ? null : inputRow;
        }
        if (!this.config.getIncludeKinds().isEmpty()) {
            return this.config.getIncludeKinds().contains(inputRow.getRowKind()) ? inputRow : null;
        }
        throw new SeaTunnelRuntimeException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "Transform config error! Either excludeKinds or includeKinds must be configured");
    }
}
