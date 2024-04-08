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

import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig.WAREHOUSE;

@Slf4j
public class PaimonCatalogLoader implements Serializable {
    /** hdfs uri is required */
    private static final String HDFS_DEF_FS_NAME = "fs.defaultFS";

    private static final String HDFS_PREFIX = "hdfs://";
    /** ********* Hdfs constants ************* */
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";

    private static final String HDFS_IMPL_KEY = "fs.hdfs.impl";

    private String warehouse;

    private PaimonHadoopConfiguration paimonHadoopConfiguration;

    public PaimonCatalogLoader(PaimonSinkConfig paimonSinkConfig) {
        this.warehouse = paimonSinkConfig.getWarehouse();
        this.paimonHadoopConfiguration = PaimonSecurityContext.loadHadoopConfig(paimonSinkConfig);
    }

    public Catalog loadCatalog() {
        // When using the seatunel engine, set the current class loader to prevent loading failures
        Thread.currentThread().setContextClassLoader(PaimonCatalogLoader.class.getClassLoader());
        final Map<String, String> optionsMap = new HashMap<>(1);
        optionsMap.put(WAREHOUSE.key(), warehouse);
        final Options options = Options.fromMap(optionsMap);
        if (warehouse.startsWith(HDFS_PREFIX)) {
            checkConfiguration(paimonHadoopConfiguration, HDFS_DEF_FS_NAME);
            paimonHadoopConfiguration.set(HDFS_IMPL_KEY, HDFS_IMPL);
        }
        PaimonSecurityContext.shouldEnableKerberos(paimonHadoopConfiguration);
        final CatalogContext catalogContext =
                CatalogContext.create(options, paimonHadoopConfiguration);
        try {
            return PaimonSecurityContext.runSecured(
                    () -> CatalogFactory.createCatalog(catalogContext));
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.LOAD_CATALOG,
                    "Failed to perform SecurityContext.runSecured",
                    e);
        }
    }

    void checkConfiguration(Configuration configuration, String key) {
        Iterator<Map.Entry<String, String>> entryIterator = configuration.iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            if (entry.getKey().equals(key)) {
                if (StringUtils.isBlank(entry.getValue())) {
                    throw new IllegalArgumentException("The value of" + key + " is required");
                }
                return;
            }
        }
        throw new IllegalArgumentException(key + " is required");
    }
}
