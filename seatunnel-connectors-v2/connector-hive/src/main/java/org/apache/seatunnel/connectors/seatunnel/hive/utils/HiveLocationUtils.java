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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utils to resolve default Hive table LOCATION. Qualifies to HDFS if fs.defaultFS is hdfs://,
 * otherwise falls back to local file path under /tmp.
 */
public final class HiveLocationUtils {

    private HiveLocationUtils() {}

    public static String qualifiedDefaultLocation(
            ReadonlyConfig config, String database, String table) {
        String confDir = config.getOptional(HiveConfig.HADOOP_CONF_PATH).orElse(null);
        String hiveSite = config.getOptional(HiveConfig.HIVE_SITE_PATH).orElse(null);
        return qualifiedDefaultLocation(confDir, hiveSite, database, table);
    }

    public static String qualifiedDefaultLocation(
            String hadoopConfDir, String hiveSitePath, String database, String table) {
        try {
            org.apache.hadoop.conf.Configuration conf =
                    new org.apache.hadoop.conf.Configuration(false);

            if (hadoopConfDir != null && !hadoopConfDir.isEmpty()) {
                String[] files = new String[] {"core-site.xml", "hdfs-site.xml"};
                for (String f : files) {
                    Path p = Paths.get(hadoopConfDir, f);
                    if (Files.exists(p)) {
                        conf.addResource(p.toUri().toURL());
                    }
                }
            }
            if (hiveSitePath != null && !hiveSitePath.isEmpty()) {
                File f = new File(hiveSitePath);
                if (f.exists()) {
                    conf.addResource(f.toURI().toURL());
                }
            }

            String defaultFs = conf.get("fs.defaultFS");
            String warehouse = conf.get("hive.metastore.warehouse.dir");
            if (warehouse == null) {
                warehouse = conf.get("metastore.warehouse.dir");
            }

            if (defaultFs != null && defaultFs.toLowerCase().startsWith("hdfs://")) {
                String base =
                        (warehouse != null && !warehouse.isEmpty())
                                ? warehouse
                                : "/user/hive/warehouse";
                String suffix = String.format("/%s.db/%s", database, table);
                if (base.contains("://")) {
                    return trimTrailingSlash(base) + suffix;
                } else {
                    String prefix = trimTrailingSlash(defaultFs);
                    String joined =
                            prefix + (base.startsWith("/") ? "" : "/") + trimTrailingSlash(base);
                    return joined + suffix;
                }
            }
        } catch (Exception ignored) {
            // Fallback below
        }
        return String.format("file:/tmp/hive/warehouse/%s.db/%s", database, table);
    }

    private static String trimTrailingSlash(String s) {
        if (s == null) return null;
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '/') end--;
        return (end == s.length()) ? s : s.substring(0, end);
    }
}
