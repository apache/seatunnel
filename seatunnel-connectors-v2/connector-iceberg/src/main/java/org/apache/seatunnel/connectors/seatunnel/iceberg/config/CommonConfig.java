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
import org.apache.seatunnel.common.config.ConfigRuntimeException;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Getter
@ToString
public class CommonConfig implements Serializable {
    private static final long serialVersionUID = 239821141534421580L;

    public static final Option<String> KEY_CATALOG_NAME =
            Options.key("catalog_name")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(" the iceberg catalog name");

    public static final Option<String> KEY_NAMESPACE =
            Options.key("namespace")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(" the iceberg namespace");

    public static final Option<String> KEY_TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" the iceberg table");

    public static final Option<Map<String, String>> CATALOG_PROPS =
            Options.key("iceberg.catalog.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the properties for initializing the Iceberg catalog, which can be referenced in this file:'https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java'");

    public static final Option<Map<String, String>> HADOOP_PROPS =
            Options.key("hadoop.config")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("Properties passed through to the Hadoop configuration");

    public static final Option<String> HADOOP_CONF_PATH_PROP =
            Options.key("iceberg.hadoop-conf-path")
                    .stringType()
                    .defaultValue(null)
                    .withDescription(
                            "The specified loading paths for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files.");

    public static final Option<Boolean> KEY_CASE_SENSITIVE =
            Options.key("case_sensitive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(" the iceberg case_sensitive");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When use kerberos, we should set kerberos user principal");

    public static final Option<String> KRB5_PATH =
            Options.key("krb5_path")
                    .stringType()
                    .defaultValue("/etc/krb5.conf")
                    .withDescription(
                            "When use kerberos, we should set krb5 path file path such as '/seatunnel/krb5.conf' or use the default path '/etc/krb5.conf'");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When using kerberos, We should specify the keytab path");

    private String catalogName;
    private String namespace;
    private String table;
    private boolean caseSensitive;

    private Map<String, String> catalogProps;
    private Map<String, String> hadoopProps;
    private String hadoopConfPath;

    // kerberos

    private String kerberosPrincipal;
    private String kerberosKeytabPath;
    private String kerberosKrb5ConfPath;

    public CommonConfig(ReadonlyConfig pluginConfig) {
        this.catalogName = checkArgumentNotNull(pluginConfig.get(KEY_CATALOG_NAME));
        this.namespace = pluginConfig.get(KEY_NAMESPACE);
        this.table = pluginConfig.get(KEY_TABLE);
        this.catalogProps = pluginConfig.get(CATALOG_PROPS);
        this.hadoopProps = pluginConfig.get(HADOOP_PROPS);
        this.hadoopConfPath = pluginConfig.get(HADOOP_CONF_PATH_PROP);
        if (pluginConfig.toConfig().hasPath(KEY_CASE_SENSITIVE.key())) {
            this.caseSensitive = pluginConfig.get(KEY_CASE_SENSITIVE);
        }
        if (pluginConfig.getOptional(KERBEROS_PRINCIPAL).isPresent()) {
            this.kerberosPrincipal = pluginConfig.getOptional(KERBEROS_PRINCIPAL).get();
        }
        if (pluginConfig.getOptional(KRB5_PATH).isPresent()) {
            this.kerberosKrb5ConfPath = pluginConfig.getOptional(KRB5_PATH).get();
        }
        if (pluginConfig.getOptional(KERBEROS_KEYTAB_PATH).isPresent()) {
            this.kerberosKeytabPath = pluginConfig.getOptional(KERBEROS_KEYTAB_PATH).get();
        }
        validate();
    }

    protected <T> T checkArgumentNotNull(T argument) {
        checkNotNull(argument);
        return argument;
    }

    private void validate() {
        checkState(!catalogProps.isEmpty(), "Must specify iceberg catalog config");
    }

    private void checkState(boolean condition, String msg) {
        if (!condition) {
            throw new ConfigRuntimeException(msg);
        }
    }
}
