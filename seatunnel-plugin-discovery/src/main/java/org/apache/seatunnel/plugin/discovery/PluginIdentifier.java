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

package org.apache.seatunnel.plugin.discovery;

import org.apache.commons.lang3.StringUtils;

/**
 * Used to identify a plugin.
 */
public class PluginIdentifier {
    private final String engineType;
    private final String pluginType;
    private final String pluginName;

    private PluginIdentifier(String engineType, String pluginType, String pluginName) {
        this.engineType = engineType;
        this.pluginType = pluginType;
        this.pluginName = pluginName;
    }

    public static PluginIdentifier of(String engineType, String pluginType, String pluginName) {
        return new PluginIdentifier(engineType, pluginType, pluginName);
    }

    public String getEngineType() {
        return engineType;
    }

    public String getPluginType() {
        return pluginType;
    }

    public String getPluginName() {
        return pluginName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PluginIdentifier that = (PluginIdentifier) o;

        if (!StringUtils.equalsIgnoreCase(engineType, that.engineType)) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(pluginType, that.pluginType)) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(pluginName, that.pluginName);
    }

    @Override
    public int hashCode() {
        int result = engineType != null ? engineType.toLowerCase().hashCode() : 0;
        result = 31 * result + (pluginType != null ? pluginType.toLowerCase().hashCode() : 0);
        result = 31 * result + (pluginName != null ? pluginName.toLowerCase().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PluginIdentifier{" +
            "engineType='" + engineType + '\'' +
            ", pluginType='" + pluginType + '\'' +
            ", pluginName='" + pluginName + '\'' +
            '}';
    }
}
