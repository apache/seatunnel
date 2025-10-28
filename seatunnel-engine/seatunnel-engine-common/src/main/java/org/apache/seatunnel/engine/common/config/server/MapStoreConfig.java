/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

@Data
public class MapStoreConfig implements Serializable {
    private boolean mapStoreEnabled =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_ENABLED.defaultValue();

    private String mapStoreType =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_TYPE.defaultValue();
    private String namespace =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_NAMESPACE.defaultValue();
    private String clusterName =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_CLUSTER_NAME.defaultValue();

    private String defaultFS =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_DEFAULT_FS.defaultValue();
    private int blockSize =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_BLOCK_SIZE.defaultValue();
    private String ossBucket =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_BUCKET.defaultValue();
    private String ossAccessKeyId =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ACCESS_KEY_ID
                    .defaultValue();
    private String ossAccessKeySecret =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ACCESS_KEY_SECRET
                    .defaultValue();
    private String ossEndpoint =
            ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ENDPOINT.defaultValue();

    public void setMapStoreEnabled(boolean mapStoreEnabled) {
        this.mapStoreEnabled = mapStoreEnabled;
    }

    public void setMapStoreType(String mapStoreType) {
        this.mapStoreType = mapStoreType;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    public void setBlockSize(int blockSize) {
        checkPositive(
                blockSize,
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_BLOCK_SIZE
                        + " must be > 0");
        this.blockSize = blockSize;
    }

    public void setOssBucket(String ossBucket) {
        this.ossBucket = ossBucket;
    }

    public void setOssAccessKeyId(String ossAccessKeyId) {
        this.ossAccessKeyId = ossAccessKeyId;
    }

    public void setOssAccessKeySecret(String ossAccessKeySecret) {
        this.ossAccessKeySecret = ossAccessKeySecret;
    }

    public void setOssEndpoint(String ossEndpoint) {
        this.ossEndpoint = ossEndpoint;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> configMap = new java.util.HashMap<>();
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_ENABLED.key(),
                mapStoreEnabled);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_TYPE.key(), mapStoreType);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_NAMESPACE.key(), namespace);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_CLUSTER_NAME.key(),
                clusterName);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_DEFAULT_FS.key(),
                defaultFS);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_BLOCK_SIZE.key(),
                blockSize);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_BUCKET.key(),
                ossBucket);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ACCESS_KEY_ID.key(),
                ossAccessKeyId);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ACCESS_KEY_SECRET.key(),
                ossAccessKeySecret);
        configMap.put(
                ServerConfigOptions.MasterServerConfigOptions.MAP_STORE_OSS_ENDPOINT.key(),
                ossEndpoint);
        return configMap;
    }
}
