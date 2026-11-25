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

package org.apache.seatunnel.connectors.seatunnel.lance.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.catalog.LanceNamespaceType;

import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceCommonConfig implements Serializable {

    public static final String CONNECTOR_IDENTITY = "Lance";

    private LanceNamespaceType namespaceType;

    private String datasetPath;

    private Map<String, String> namespaceProps;

    private String table;

    private List<String> namespaceIds;

    private String rootNamespacePath;

    private Map<String, String> storageOptions;

    public LanceCommonConfig(LanceNamespaceType namespaceType, Map<String, String> namespaceProps) {
        this.namespaceType = namespaceType;
        this.namespaceProps = namespaceProps;
    }

    public LanceCommonConfig(
            LanceNamespaceType namespaceType,
            String datasetPath,
            Map<String, String> namespaceProps) {
        this.namespaceType = namespaceType;
        this.datasetPath = datasetPath;
        this.namespaceProps = namespaceProps;
    }

    public LanceCommonConfig(ReadonlyConfig pluginConfig) {
        this.namespaceIds = pluginConfig.get(LanceCommonOptions.KEY_NAMESPACE_IDS);
        this.table = pluginConfig.get(LanceCommonOptions.KEY_TABLE);
        this.datasetPath = pluginConfig.get(LanceCommonOptions.KEY_DATASET_PATH);
        this.rootNamespacePath = pluginConfig.get(LanceCommonOptions.KEY_ROOT_NAMESPACE_PATH);
        this.namespaceType =
                LanceNamespaceType.typeOf(pluginConfig.get(LanceCommonOptions.KEY_NAMESPACE_TYPE));
        this.storageOptions = pluginConfig.get(LanceSinkOptions.WRITE_STORAGE_OPTIONS);
    }

    public LanceNamespaceType getNamespaceType() {
        return namespaceType;
    }

    public void setNamespaceType(LanceNamespaceType namespaceType) {
        this.namespaceType = namespaceType;
    }

    public Map<String, String> getNamespaceProps() {
        return namespaceProps;
    }

    public void setNamespaceProps(Map<String, String> namespaceProps) {
        this.namespaceProps = namespaceProps;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public void setDatasetPath(String datasetPath) {
        this.datasetPath = datasetPath;
    }

    public String getTable() {
        return table;
    }

    public List<String> getNamespaceIds() {
        return namespaceIds;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setNamespaceIds(List<String> namespaceIds) {
        this.namespaceIds = namespaceIds;
    }

    public String getRootNamespacePath() {
        return rootNamespacePath;
    }

    public void setRootNamespacePath(String rootNamespacePath) {
        this.rootNamespacePath = rootNamespacePath;
    }

    /**
     * Storage options configured through lance.write.storage.options, shared by the sink writer and
     * the catalog.
     */
    public Map<String, String> getStorageOptions() {
        return storageOptions;
    }

    public void setStorageOptions(Map<String, String> storageOptions) {
        this.storageOptions = storageOptions;
    }

    /**
     * Read options carrying the configured storage credentials. lance-core resolves credentials per
     * call instead of caching them, so every remote dataset open must pass these options; unset
     * options become an empty map and local paths keep working unchanged.
     */
    public ReadOptions toReadOptions() {
        return new ReadOptions.Builder()
                .setStorageOptions(storageOptions == null ? new HashMap<>() : storageOptions)
                .build();
    }

    /**
     * Write params carrying the configured storage credentials for every remote dataset create and
     * fragment write; unset options become an empty map for local paths.
     */
    public WriteParams toWriteParams() {
        return new WriteParams.Builder()
                .withStorageOptions(storageOptions == null ? new HashMap<>() : storageOptions)
                .build();
    }
}
