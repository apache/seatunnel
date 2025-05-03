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

import java.util.HashMap;
import java.util.Map;

public class IcebergCommonOptions {

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
}
