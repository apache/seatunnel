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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.engine.common.config.server.MapStoreConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class MapStoreConfigFactory {

    public static final String ENGINE_MAP_NAME = "engine*";

    private MapStoreConfigFactory() {}

    public static MapStoreConfig createMapStoreConfig(
            MapStoreConfig mapStoreConfig, HazelcastInstance hazelcastInstance) {
        MapConfig mapConfig = hazelcastInstance.getConfig().getMapConfig(ENGINE_MAP_NAME);
        if (mapConfig == null || mapConfig.getMapStoreConfig() == null) {
            return mapStoreConfig;
        }

        Properties mapStoreProperties = mapConfig.getMapStoreConfig().getProperties();
        if (mapStoreProperties == null || mapStoreProperties.isEmpty()) {
            return mapStoreConfig;
        }

        Map<String, Object> config = new HashMap<>(Maps.fromProperties(mapStoreProperties));

        try {
            Object mapStoreEnabled =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_ENABLED.key());
            if (mapStoreEnabled != null) {
                mapStoreConfig.setMapStoreEnabled(Boolean.parseBoolean(mapStoreEnabled.toString()));
            }

            Object mapStoreType =
                    config.get(ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_TYPE.key());
            if (mapStoreType != null) {
                mapStoreConfig.setMapStoreType(mapStoreType.toString());
            }

            Object namespace =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_NAMESPACE
                                    .key());
            if (namespace != null) {
                mapStoreConfig.setNamespace(namespace.toString());
            }

            Object clusterName =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_CLUSTER_NAME
                                    .key());
            if (clusterName != null) {
                mapStoreConfig.setClusterName(clusterName.toString());
            }

            Object defaultFS =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_DEFAULT_FS
                                    .key());
            if (defaultFS != null) {
                mapStoreConfig.setDefaultFS(defaultFS.toString());
            }

            Object blockSize =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_BLOCK_SIZE
                                    .key());
            if (blockSize != null) {
                mapStoreConfig.setBlockSize(Integer.parseInt(blockSize.toString()));
            }

            Object ossBucket =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_BUCKET
                                    .key());
            if (ossBucket != null) {
                mapStoreConfig.setOssBucket(ossBucket.toString());
            }

            Object ossAccessKeyId =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions
                                    .MAP_STORE_OSS_ACCESS_KEY_ID
                                    .key());
            if (ossAccessKeyId != null) {
                mapStoreConfig.setOssAccessKeyId(ossAccessKeyId.toString());
            }

            Object ossAccessKeySecret =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions
                                    .MAP_STORE_OSS_ACCESS_KEY_SECRET
                                    .key());
            if (ossAccessKeySecret != null) {
                mapStoreConfig.setOssAccessKeySecret(ossAccessKeySecret.toString());
            }

            Object ossEndpoint =
                    config.get(
                            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ENDPOINT
                                    .key());
            if (ossEndpoint != null) {
                mapStoreConfig.setOssEndpoint(ossEndpoint.toString());
            }
        } catch (Exception e) {
            log.warn("Failed to create MapStoreConfig from hazelcast config map", e);
        }

        return mapStoreConfig;
    }
}
