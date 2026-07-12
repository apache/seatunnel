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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import lombok.Data;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.ZoneId;

@Data
public class HugeGraphSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private HugeGraphConnectionConfig connectionConfig;
    private String label;
    private MappingConfig.LabelType labelType;
    private SeaTunnelRowType schema;
    private int pageSize;
    private String timeZone;

    public static HugeGraphSourceConfig of(ReadonlyConfig config, SeaTunnelRowType schema) {
        HugeGraphSourceConfig sourceConfig = new HugeGraphSourceConfig();
        sourceConfig.setConnectionConfig(HugeGraphConnectionConfig.of(config));
        sourceConfig.setLabel(config.get(HugeGraphSourceOptions.LABEL));
        sourceConfig.setLabelType(
                config.getOptional(HugeGraphSourceOptions.LABEL_TYPE)
                        .orElse(HugeGraphSourceOptions.LABEL_TYPE.defaultValue()));
        sourceConfig.setSchema(schema);
        sourceConfig.setPageSize(
                config.getOptional(HugeGraphSourceOptions.PAGE_SIZE)
                        .orElse(HugeGraphSourceOptions.PAGE_SIZE.defaultValue()));
        config.getOptional(HugeGraphSourceOptions.TIME_ZONE).ifPresent(sourceConfig::setTimeZone);
        validate(sourceConfig);
        return sourceConfig;
    }

    private static void validate(HugeGraphSourceConfig sourceConfig) {
        int pageSize = sourceConfig.getPageSize();
        if (pageSize < HugeGraphSourceOptions.MIN_PAGE_SIZE
                || pageSize > HugeGraphSourceOptions.MAX_PAGE_SIZE) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Option 'page_size' must be in range [%s, %s], but got %s",
                            HugeGraphSourceOptions.MIN_PAGE_SIZE,
                            HugeGraphSourceOptions.MAX_PAGE_SIZE,
                            pageSize));
        }

        // schema must be present, but an empty fields block is valid: a property-less label
        // (e.g. a pure relationship edge, or a vertex with no properties) is exported as just the
        // reserved columns (~id/~label/…). Requiring a fake property would make such labels
        // unreadable.
        if (sourceConfig.getSchema() == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'schema' is required (use 'schema = { fields {} }' for a label with no properties)");
        }
        if (sourceConfig.getTimeZone() != null) {
            try {
                ZoneId.of(sourceConfig.getTimeZone());
            } catch (DateTimeException e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Option 'time_zone' must be a valid ZoneId, but got '%s'",
                                sourceConfig.getTimeZone()),
                        e);
            }
        }
    }
}
