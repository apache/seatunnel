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

package org.apache.seatunnel.flink.hive.source;

import static org.apache.seatunnel.flink.hive.config.HiveConfig.DATABASE;
import static org.apache.seatunnel.flink.hive.config.HiveConfig.HIVE_CATALOG_NAME;
import static org.apache.seatunnel.flink.hive.config.HiveConfig.HIVE_CONF_DIR;
import static org.apache.seatunnel.flink.hive.config.HiveConfig.PRE_SQL;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

@AutoService(BaseFlinkSource.class)
public class HiveSource implements FlinkBatchSource {
    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        BatchTableEnvironment batchTableEnvironment = env.getBatchTableEnvironment();
        TableResult tableResult = batchTableEnvironment.executeSql(config.getString(PRE_SQL));
        Table table = batchTableEnvironment.fromValues(tableResult.collect());
        return batchTableEnvironment.toDataSet(table, Row.class);
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, PRE_SQL, DATABASE);
    }

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        BatchTableEnvironment batchTableEnvironment = env.getBatchTableEnvironment();
        HiveCatalog hiveCatalog = new HiveCatalog(HIVE_CATALOG_NAME, config.getString(DATABASE), config.getString(HIVE_CONF_DIR));
        batchTableEnvironment.registerCatalog(HIVE_CATALOG_NAME, hiveCatalog);
        batchTableEnvironment.useCatalog(HIVE_CATALOG_NAME);
        batchTableEnvironment.useDatabase(config.getString(DATABASE));
    }
}
