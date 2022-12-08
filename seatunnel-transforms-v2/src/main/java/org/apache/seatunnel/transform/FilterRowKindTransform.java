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
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.common.FilterRowTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.ToString;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ToString(of = {"includeKinds", "excludeKinds"})
@AutoService(SeaTunnelTransform.class)
public class FilterRowKindTransform extends FilterRowTransform {
    public static final Option<List<RowKind>> INCLUDE_KINDS = Options.key("include_kinds")
        .listType(RowKind.class)
        .noDefaultValue()
        .withDescription("the row kinds to include");
    public static final Option<List<RowKind>> EXCLUDE_KINDS = Options.key("exclude_kinds")
        .listType(RowKind.class)
        .noDefaultValue()
        .withDescription("the row kinds to exclude");

    private Set<RowKind> includeKinds = Collections.emptySet();
    private Set<RowKind> excludeKinds = Collections.emptySet();

    @Override
    public String getPluginName() {
        return "FilterRowKind";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(INCLUDE_KINDS.key())) {
            includeKinds = new HashSet<>(pluginConfig.getEnumList(RowKind.class, INCLUDE_KINDS.key()));
        }
        if (pluginConfig.hasPath(EXCLUDE_KINDS.key())) {
            excludeKinds = new HashSet<>(pluginConfig.getEnumList(RowKind.class, EXCLUDE_KINDS.key()));
        }
        if ((includeKinds.isEmpty() && excludeKinds.isEmpty())
            || (!includeKinds.isEmpty() && !excludeKinds.isEmpty())) {
            throw new SeaTunnelRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT,
                String.format("These options(%s,%s) are mutually exclusive, allowing only one set of options to be configured.",
                    INCLUDE_KINDS.key(), EXCLUDE_KINDS.key()));
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (!excludeKinds.isEmpty()) {
            return excludeKinds.contains(inputRow.getRowKind()) ? null : inputRow;
        }
        if (!includeKinds.isEmpty()) {
            return includeKinds.contains(inputRow.getRowKind()) ? inputRow : null;
        }
        throw new SeaTunnelRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION,
            "Transform config error! Either excludeKinds or includeKinds must be configured");
    }
}
