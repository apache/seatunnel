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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;

@Getter
@ToString
public class CommonConfig implements Serializable {
    private static final long serialVersionUID = 239821141534421580L;

    public static final Option<String> KEY_CATALOG_NAME =
            Options.key("catalog_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" the iceberg catalog name");

    public static final Option<IcebergCatalogType> KEY_CATALOG_TYPE =
            Options.key("catalog_type")
                    .enumType(IcebergCatalogType.class)
                    .noDefaultValue()
                    .withDescription(" the iceberg catalog type");

    public static final Option<String> KEY_NAMESPACE =
            Options.key("namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" the iceberg namespace");

    public static final Option<String> KEY_TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" the iceberg table");

    public static final Option<String> KEY_URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" the iceberg server uri");

    public static final Option<String> KEY_WAREHOUSE =
            Options.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" the iceberg warehouse");

    public static final Option<Boolean> KEY_CASE_SENSITIVE =
            Options.key("case_sensitive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(" the iceberg case_sensitive");

    public static final Option<List<String>> KEY_FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(" the iceberg table fields");

    // for kerberos
    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("jdbc kerberos_principal");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("jdbc kerberos_keytab_path");

    public static final Option<String> KERBEROS_KRB5_CONF_PATH =
            Options.key("kerberos_krb5_conf_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("jdbc kerberos_keytab_path");

    public static final Option<String> HDFS_SITE_PATH =
            Options.key("hdfs_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("jdbc hdfs_site_path");

    public static final Option<String> HIVE_SITE_PATH =
            Options.key("hive_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("jdbc hive_site_path");

    private String catalogName;
    private IcebergCatalogType catalogType;
    private String uri;
    private String warehouse;
    private String namespace;
    private String table;
    private boolean caseSensitive;

    // kerberos
    private String kerberosPrincipal;
    private String kerberosKeytabPath;
    private String kerberosKrb5ConfPath;
    private String hdfsSitePath;
    private String hiveSitePath;

    public CommonConfig(ReadonlyConfig pluginConfig) {
        IcebergCatalogType catalogType = pluginConfig.getOptional(KEY_CATALOG_TYPE).get();
        checkArgument(
                HADOOP.equals(catalogType) || HIVE.equals(catalogType),
                "Illegal catalogType: " + catalogType);

        this.catalogType = catalogType;
        this.catalogName = pluginConfig.getOptional(KEY_CATALOG_NAME).get();
        if (pluginConfig.getOptional(KEY_URI).isPresent()) {
            this.uri = pluginConfig.getOptional(KEY_URI).get();
        }
        this.warehouse = pluginConfig.getOptional(KEY_WAREHOUSE).get();
        this.namespace = pluginConfig.getOptional(KEY_NAMESPACE).get();
        this.table = pluginConfig.getOptional(KEY_TABLE).get();

        if (pluginConfig.getOptional(KEY_CASE_SENSITIVE).isPresent()) {
            this.caseSensitive = pluginConfig.getOptional(KEY_CASE_SENSITIVE).get();
        }

        if (pluginConfig.getOptional(KERBEROS_PRINCIPAL).isPresent()) {
            this.kerberosPrincipal = pluginConfig.getOptional(KERBEROS_PRINCIPAL).get();
        }
        if (pluginConfig.getOptional(KERBEROS_KEYTAB_PATH).isPresent()) {
            this.kerberosKeytabPath = pluginConfig.getOptional(KERBEROS_KEYTAB_PATH).get();
        }
        if (pluginConfig.getOptional(KERBEROS_KRB5_CONF_PATH).isPresent()) {
            this.kerberosKrb5ConfPath = pluginConfig.getOptional(KERBEROS_KRB5_CONF_PATH).get();
        }
        if (pluginConfig.getOptional(HDFS_SITE_PATH).isPresent()) {
            this.hdfsSitePath = pluginConfig.getOptional(HDFS_SITE_PATH).get();
        }
        if (pluginConfig.getOptional(HIVE_SITE_PATH).isPresent()) {
            this.hiveSitePath = pluginConfig.getOptional(HIVE_SITE_PATH).get();
        }
    }

    protected <T> T checkArgumentNotNull(T argument) {
        checkNotNull(argument);
        return argument;
    }
}
