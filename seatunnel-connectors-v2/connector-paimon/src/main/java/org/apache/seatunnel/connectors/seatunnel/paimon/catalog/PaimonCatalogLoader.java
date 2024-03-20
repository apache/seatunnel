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

package org.apache.seatunnel.connectors.seatunnel.paimon.catalog;

import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.WAREHOUSE;

@Slf4j
public class PaimonCatalogLoader implements Serializable {
    private PaimonSinkConfig config;

    public PaimonCatalogLoader(PaimonSinkConfig config) {
        this.config = config;
    }

    public Catalog loadCatalog() {
        // When using the seatunel engine, set the current class loader to prevent loading failures
        Thread.currentThread().setContextClassLoader(PaimonCatalogLoader.class.getClassLoader());
        final String warehouse = config.getWarehouse();
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(WAREHOUSE.key(), warehouse);
        final Options options = Options.fromMap(optionsMap);
        final Configuration hadoopConf = new Configuration();
        String hdfsSitePathOptional = config.getHdfsSitePath();
        if (StringUtils.isNotBlank(hdfsSitePathOptional)) {
            hadoopConf.addResource(new Path(hdfsSitePathOptional));
        }
        final CatalogContext catalogContext = CatalogContext.create(options, hadoopConf);
        return CatalogFactory.createCatalog(catalogContext);
    }
}
