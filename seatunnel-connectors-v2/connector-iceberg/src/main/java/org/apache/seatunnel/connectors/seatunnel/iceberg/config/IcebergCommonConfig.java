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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Getter
@ToString
public class IcebergCommonConfig implements Serializable {
    private static final long serialVersionUID = 239821141534421580L;

    private String catalogName;
    private String namespace;
    private String table;

    private Map<String, String> catalogProps;
    private Map<String, String> hadoopProps;
    private String hadoopConfPath;
    private boolean caseSensitive;

    // kerberos
    private String kerberosPrincipal;
    private String kerberosKeytabPath;
    private String kerberosKrb5ConfPath;

    public IcebergCommonConfig(ReadonlyConfig pluginConfig) {
        this.catalogName = pluginConfig.get(IcebergCommonOptions.KEY_CATALOG_NAME);
        this.namespace = pluginConfig.get(IcebergCommonOptions.KEY_NAMESPACE);
        this.table = pluginConfig.get(IcebergCommonOptions.KEY_TABLE);
        this.catalogProps = pluginConfig.get(IcebergCommonOptions.CATALOG_PROPS);
        this.hadoopProps = pluginConfig.get(IcebergCommonOptions.HADOOP_PROPS);
        this.hadoopConfPath = pluginConfig.get(IcebergCommonOptions.HADOOP_CONF_PATH_PROP);
        this.caseSensitive = pluginConfig.get(IcebergCommonOptions.KEY_CASE_SENSITIVE);
        if (pluginConfig.getOptional(IcebergCommonOptions.KERBEROS_PRINCIPAL).isPresent()) {
            this.kerberosPrincipal = pluginConfig.get(IcebergCommonOptions.KERBEROS_PRINCIPAL);
        }
        if (pluginConfig.getOptional(IcebergCommonOptions.KRB5_PATH).isPresent()) {
            this.kerberosKrb5ConfPath = pluginConfig.get(IcebergCommonOptions.KRB5_PATH);
        }
        if (pluginConfig.getOptional(IcebergCommonOptions.KERBEROS_KEYTAB_PATH).isPresent()) {
            this.kerberosKeytabPath = pluginConfig.get(IcebergCommonOptions.KERBEROS_KEYTAB_PATH);
        }
    }
}
