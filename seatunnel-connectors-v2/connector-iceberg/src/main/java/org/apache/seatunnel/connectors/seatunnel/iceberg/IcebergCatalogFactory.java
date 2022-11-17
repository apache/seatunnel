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

package org.apache.seatunnel.connectors.seatunnel.iceberg;

import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;

import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class IcebergCatalogFactory implements Serializable {

    private static final long serialVersionUID = -6003040601422350869L;

    private final String catalogName;
    private final IcebergCatalogType catalogType;
    private final String warehouse;
    private final String uri;

    public IcebergCatalogFactory(@NonNull String catalogName,
                                 @NonNull IcebergCatalogType catalogType,
                                 @NonNull String warehouse,
                                 String uri) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
        this.warehouse = warehouse;
        this.uri = uri;
    }

    public Catalog create() {
        Configuration conf = new Configuration();
        SerializableConfiguration serializableConf = new SerializableConfiguration(conf);
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        switch (catalogType) {
            case HADOOP:
                return hadoop(catalogName, serializableConf, properties);
            case HIVE:
                properties.put(CatalogProperties.URI, uri);
                return hive(catalogName, serializableConf, properties);
            default:
                throw new UnsupportedOperationException("Unsupported catalogType: " + catalogType);
        }
    }

    private static Catalog hadoop(String catalogName,
                                  SerializableConfiguration conf,
                                  Map<String, String> properties) {
        return CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), catalogName, properties, conf.get());
    }

    private static Catalog hive(String catalogName,
                                SerializableConfiguration conf,
                                Map<String, String> properties) {
        return CatalogUtil.loadCatalog(HiveCatalog.class.getName(), catalogName, properties, conf.get());
    }
}
