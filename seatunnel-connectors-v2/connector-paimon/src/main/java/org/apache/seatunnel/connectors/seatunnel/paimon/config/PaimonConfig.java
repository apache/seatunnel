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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogEnum;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Utility class to store configuration options, used by {@link SeaTunnelSource} and {@link
 * SeaTunnelSink}.
 */
@Getter
public class PaimonConfig implements Serializable {

    protected String catalogName;
    protected PaimonCatalogEnum catalogType;
    protected String catalogUri;
    protected String warehouse;
    protected String namespace;
    protected String table;
    protected String hdfsSitePath;
    protected Map<String, String> hadoopConfProps;
    protected String hadoopConfPath;

    public PaimonConfig(ReadonlyConfig readonlyConfig) {
        this.catalogName =
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.CATALOG_NAME),
                        PaimonBaseOptions.CATALOG_NAME.key());
        this.warehouse =
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.WAREHOUSE),
                        PaimonBaseOptions.WAREHOUSE.key());
        this.namespace =
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.DATABASE),
                        PaimonBaseOptions.DATABASE.key());
        this.table =
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.TABLE), PaimonBaseOptions.TABLE.key());
        this.hdfsSitePath = readonlyConfig.get(PaimonBaseOptions.HDFS_SITE_PATH);
        this.hadoopConfProps = readonlyConfig.get(PaimonBaseOptions.HADOOP_CONF);
        this.hadoopConfPath = readonlyConfig.get(PaimonBaseOptions.HADOOP_CONF_PATH);
        this.catalogType = readonlyConfig.get(PaimonBaseOptions.CATALOG_TYPE);
        if (PaimonCatalogEnum.HIVE.getType().equals(catalogType.getType())) {
            this.catalogUri =
                    checkArgumentNotBlank(
                            readonlyConfig.get(PaimonBaseOptions.CATALOG_URI),
                            PaimonBaseOptions.CATALOG_URI.key());
        }
    }

    protected String checkArgumentNotBlank(String propValue, String propKey) {
        if (StringUtils.isBlank(propValue)) {
            throw new SeaTunnelException(
                    CommonError.convertToConnectorPropsBlankError("Paimon", propKey));
        }
        return propValue;
    }

    protected static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
