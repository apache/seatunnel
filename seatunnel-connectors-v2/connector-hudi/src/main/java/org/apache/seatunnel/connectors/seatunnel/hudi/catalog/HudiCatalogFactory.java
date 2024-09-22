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

package org.apache.seatunnel.connectors.seatunnel.hudi.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;

import org.apache.hadoop.conf.Configuration;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.CONF_FILES_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiOptions.TABLE_DFS_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil.getConfiguration;

@AutoService(Factory.class)
public class HudiCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        Configuration hadoopConf = getConfiguration(options.get(CONF_FILES_PATH));
        String tableDfsPath = options.get(TABLE_DFS_PATH);
        return new HudiCatalog(catalogName, hadoopConf, tableDfsPath);
    }

    @Override
    public String factoryIdentifier() {
        return "Hudi";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(TABLE_DFS_PATH).optional(CONF_FILES_PATH).build();
    }
}
