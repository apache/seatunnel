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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@ToString
public class CommonConfig implements Serializable {
    private static final long serialVersionUID = 239821141534421580L;

    private static final String KEY_CATALOG_NAME = "catalog_name";
    private static final String KEY_CATALOG_TYPE = "catalog_type";
    private static final String KEY_NAMESPACE = "namespace";
    private static final String KEY_TABLE = "table";
    private static final String KEY_URI = "uri";
    private static final String KEY_WAREHOUSE = "warehouse";
    private static final String KEY_CASE_SENSITIVE = "case_sensitive";

    public static final String KEY_FIELDS = "fields";
    public static final String CATALOG_TYPE_HADOOP = "hadoop";
    public static final String CATALOG_TYPE_HIVE = "hive";

    private String catalogName;
    private String catalogType;
    private String uri;
    private String warehouse;
    private String namespace;
    private String table;
    private boolean caseSensitive;

    public CommonConfig(Config pluginConfig) {
        String catalogType = checkArgumentNotNull(pluginConfig.getString(KEY_CATALOG_TYPE));
        checkArgument(CATALOG_TYPE_HADOOP.equals(catalogType)
                || CATALOG_TYPE_HIVE.equals(catalogType),
            "Illegal catalogType: " + catalogType);

        this.catalogType = catalogType;
        this.catalogName = checkArgumentNotNull(pluginConfig.getString(KEY_CATALOG_NAME));
        if (pluginConfig.hasPath(KEY_URI)) {
            this.uri = checkArgumentNotNull(pluginConfig.getString(KEY_URI));
        }
        this.warehouse = checkArgumentNotNull(pluginConfig.getString(KEY_WAREHOUSE));
        this.namespace = checkArgumentNotNull(pluginConfig.getString(KEY_NAMESPACE));
        this.table = checkArgumentNotNull(pluginConfig.getString(KEY_TABLE));

        if (pluginConfig.hasPath(KEY_CASE_SENSITIVE)) {
            this.caseSensitive = pluginConfig.getBoolean(KEY_CASE_SENSITIVE);
        }
    }

    protected <T> T checkArgumentNotNull(T argument) {
        checkNotNull(argument);
        return argument;
    }
}
