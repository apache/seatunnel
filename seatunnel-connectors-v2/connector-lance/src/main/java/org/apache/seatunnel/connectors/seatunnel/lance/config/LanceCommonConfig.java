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

import java.io.Serializable;
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
}
