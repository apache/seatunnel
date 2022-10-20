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

package org.apache.seatunnel.flink.hive.common;

import static org.apache.seatunnel.flink.hive.config.HiveConfig.CATALOG_NAME;
import static org.apache.seatunnel.flink.hive.config.HiveConfig.DATABASE;
import static org.apache.seatunnel.flink.hive.config.HiveConfig.HIVE_CONF_DIR;
import static org.apache.seatunnel.flink.hive.config.HiveConfig.SQL;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Optional;

public class BaseHivePlugin {
    protected Config config;

    public void setConfig(Config config) {
        this.config = config;
    }

    public Config getConfig() {
        return config;
    }

    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, SQL, DATABASE, HIVE_CONF_DIR);
    }

    public void prepare(FlinkEnvironment env) {
        StreamTableEnvironment streamTableEnvironment = env.getStreamTableEnvironment();
        HiveCatalog hiveCatalog = new HiveCatalog(config.getString(CATALOG_NAME), config.getString(DATABASE), config.getString(HIVE_CONF_DIR));
        Optional<Catalog> catalog = streamTableEnvironment.getCatalog(config.getString(CATALOG_NAME));
        if (!catalog.isPresent()) {
            streamTableEnvironment.registerCatalog(config.getString(CATALOG_NAME), hiveCatalog);
            streamTableEnvironment.useCatalog(config.getString(CATALOG_NAME));
            streamTableEnvironment.useDatabase(config.getString(DATABASE));
        } else {
            throw new UnsupportedOperationException("Catalog name is duplicated");
        }
    }

    public String getPluginName() {
        return "HiveStream";
    }
}
