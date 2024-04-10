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

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class IcebergCatalogLoader implements Serializable {

    private static final long serialVersionUID = -6003040601422350869L;
    private static final List<String> HADOOP_CONF_FILES =
            ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");
    private CommonConfig config;

    public IcebergCatalogLoader(CommonConfig config) {
        this.config = config;
    }

    public Catalog loadCatalog() {
        // When using the seatunel engine, set the current class loader to prevent loading failures
        Thread.currentThread().setContextClassLoader(IcebergCatalogLoader.class.getClassLoader());
        return CatalogUtil.buildIcebergCatalog(
                config.getCatalogName(), config.getCatalogProps(), loadHadoopConfig(config));
    }

    /**
     * Loading Hadoop configuration through reflection
     *
     * @param config
     * @return
     */
    private Object loadHadoopConfig(CommonConfig config) {
        Class<?> configClass =
                DynClasses.builder()
                        .impl("org.apache.hadoop.hdfs.HdfsConfiguration")
                        .orNull()
                        .build();
        if (configClass == null) {
            configClass =
                    DynClasses.builder()
                            .impl("org.apache.hadoop.conf.Configuration")
                            .orNull()
                            .build();
        }

        if (configClass == null) {
            log.info("Hadoop not found on classpath, not creating Hadoop config");
            return null;
        }

        try {
            Object result = configClass.getDeclaredConstructor().newInstance();
            DynMethods.BoundMethod addResourceMethod =
                    DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
            DynMethods.BoundMethod setMethod =
                    DynMethods.builder("set")
                            .impl(configClass, String.class, String.class)
                            .build(result);

            //  load any config files in the specified config directory
            String hadoopConfPath = config.getHadoopConfPath();
            if (hadoopConfPath != null) {
                HADOOP_CONF_FILES.forEach(
                        confFile -> {
                            Path path = Paths.get(hadoopConfPath, confFile);
                            if (Files.exists(path)) {
                                try {
                                    addResourceMethod.invoke(path.toUri().toURL());
                                } catch (IOException e) {
                                    log.warn(
                                            "Error adding Hadoop resource {}, resource was not added",
                                            path,
                                            e);
                                }
                            }
                        });
            }
            config.getHadoopProps().forEach(setMethod::invoke);
            log.info("Hadoop config initialized: {}", configClass.getName());
            return result;
        } catch (InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            log.warn(
                    "Hadoop found on classpath but could not create config, proceeding without config",
                    e);
        }
        return null;
    }
}
