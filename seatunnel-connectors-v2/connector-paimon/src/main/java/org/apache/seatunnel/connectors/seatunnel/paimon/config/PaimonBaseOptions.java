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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogEnum;

import java.util.HashMap;
import java.util.Map;

public class PaimonBaseOptions {

    public static final String CONNECTOR_IDENTITY = "Paimon";

    public static final Option<String> WAREHOUSE =
            Options.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The warehouse path of paimon");

    public static final Option<PaimonCatalogEnum> CATALOG_TYPE =
            Options.key("catalog_type")
                    .enumType(PaimonCatalogEnum.class)
                    .defaultValue(PaimonCatalogEnum.FILESYSTEM)
                    .withDescription("The type of paimon catalog");

    public static final Option<String> CATALOG_URI =
            Options.key("catalog_uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The uri of paimon with hive catalog");

    public static final Option<String> CATALOG_NAME =
            Options.key("catalog_name")
                    .stringType()
                    .defaultValue("paimon")
                    .withDescription(" the paimon catalog name");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The database you intend to access");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table you intend to access");

    @Deprecated
    public static final Option<String> HDFS_SITE_PATH =
            Options.key("hdfs_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of hdfs-site.xml");

    public static final Option<Map<String, String>> HADOOP_CONF =
            Options.key("paimon.hadoop.conf")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("Properties in hadoop conf");

    public static final Option<String> HADOOP_CONF_PATH =
            Options.key("paimon.hadoop.conf-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The specified loading path for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files");
}
