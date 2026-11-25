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

package org.apache.seatunnel.connectors.seatunnel.lance.catalog;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonConfig;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaces;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class LanceCatalogLoader implements Serializable {

    private LanceNamespaceType namespaceType;

    private final LanceCommonConfig config;

    private BufferAllocator bufferAllocator;

    public LanceCatalogLoader(LanceCommonConfig config) {
        this.namespaceType = config.getNamespaceType();
        this.config = config;
        this.bufferAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    public LanceNamespace loadNamespace() {
        Thread.currentThread().setContextClassLoader(LanceCatalogLoader.class.getClassLoader());
        Map<String, String> properties = buildNamespaceProps();
        config.setNamespaceProps(properties);

        return LanceNamespaces.connect(
                LanceNamespaceType.ofImplByType(namespaceType.getType()),
                config.getNamespaceProps(),
                null,
                bufferAllocator);
    }

    /**
     * Builds the property map for the directory namespace. The namespace layer (lance-namespace
     * over OpenDAL) does not understand the Lance-native {@code aws_*} storage keys, so the
     * configured options are translated to their flat OpenDAL keys by {@link #translateStorageKey}
     * and prefixed with {@code storage.}. When {@code storage.options} is not configured, no {@code
     * storage.} property is added and the namespace keeps its ambient behavior.
     */
    @VisibleForTesting
    Map<String, String> buildNamespaceProps() {
        Map<String, String> properties = new HashMap<>();
        properties.put("root", config.getRootNamespacePath());
        Map<String, String> storageOptions = config.getStorageOptions();
        if (storageOptions != null) {
            for (Map.Entry<String, String> entry : storageOptions.entrySet()) {
                properties.put("storage." + translateStorageKey(entry.getKey()), entry.getValue());
            }
        }
        return properties;
    }

    /**
     * Translates the Lance-native {@code aws_*} storage keys to the flat OpenDAL keys expected by
     * the directory namespace; any other key is passed through unchanged.
     */
    private static String translateStorageKey(String key) {
        switch (key) {
            case "aws_endpoint_url":
                return "endpoint";
            case "aws_access_key_id":
                return "access_key_id";
            case "aws_secret_access_key":
                return "secret_access_key";
            case "aws_default_region":
                return "region";
            default:
                return key;
        }
    }
}
