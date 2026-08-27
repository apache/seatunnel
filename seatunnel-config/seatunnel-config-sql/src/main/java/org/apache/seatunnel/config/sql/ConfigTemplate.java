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

package org.apache.seatunnel.config.sql;

import org.apache.seatunnel.config.sql.model.Option;
import org.apache.seatunnel.config.sql.model.SeaTunnelConfig;
import org.apache.seatunnel.config.sql.model.SinkConfig;
import org.apache.seatunnel.config.sql.model.SourceConfig;
import org.apache.seatunnel.config.sql.model.TransformConfig;

import java.util.List;

public class ConfigTemplate {
    private static String globalConfig(List<String> envConfigs) {
        StringBuilder result = new StringBuilder();
        envConfigs.forEach(envConfig -> result.append(envConfig).append("\n"));
        return result.toString();
    }

    private static String sourceItems(List<SourceConfig> sourceConfigs) {
        StringBuilder sourceItems = new StringBuilder();
        for (SourceConfig sourceConfig : sourceConfigs) {
            if (sourceConfig.getOptions().isEmpty()) {
                continue;
            }
            sourceItems.append("  ").append(sourceConfig.getConnector()).append(" {\n");
            for (Option option : sourceConfig.getOptions()) {
                sourceItems
                        .append("    ")
                        .append(option.getKey())
                        .append(" = ")
                        .append(option.getValue())
                        .append("\n");
            }
            sourceItems.append("  }\n");
        }
        return sourceItems.toString();
    }

    private static String sinkItems(List<SinkConfig> sinkConfigs) {
        StringBuilder sinkItems = new StringBuilder();
        for (SinkConfig sinkConfig : sinkConfigs) {
            if (sinkConfig.getOptions().isEmpty()) {
                continue;
            }
            sinkItems.append("  ").append(sinkConfig.getConnector()).append(" {\n");
            for (Option option : sinkConfig.getOptions()) {
                sinkItems
                        .append("    ")
                        .append(option.getKey())
                        .append(" = ")
                        .append(option.getValue())
                        .append("\n");
            }
            sinkItems.append("  }\n");
        }
        return sinkItems.toString();
    }

    private static String transformItems(List<TransformConfig> transformConfigs) {
        StringBuilder transformItems = new StringBuilder();
        for (TransformConfig transformConfig : transformConfigs) {
            transformItems.append("  sql {\n");
            transformItems
                    .append("    plugin_input = \"")
                    .append(transformConfig.getPluginInputIdentifier())
                    .append("\"\n");
            transformItems
                    .append("    query = \"\"\"")
                    .append(transformConfig.getQuery())
                    .append("\"\"\"\n");
            transformItems
                    .append("    plugin_output = \"")
                    .append(transformConfig.getPluginOutputIdentifier())
                    .append("\"\n");
            transformItems.append("  }\n");
        }
        return transformItems.toString();
    }

    public static String generate(SeaTunnelConfig seaTunnelConfig) {
        String globalConfig = globalConfig(seaTunnelConfig.getEnvConfigs());

        String sourceTemplate =
                "source {\n" + sourceItems(seaTunnelConfig.getSourceConfigs()) + "}\n";
        String sinkTemplate = "sink {\n" + sinkItems(seaTunnelConfig.getSinkConfigs()) + "}\n";
        String transformTemplate =
                "transform {\n" + transformItems(seaTunnelConfig.getTransformConfigs()) + "}\n";
        return globalConfig + sourceTemplate + transformTemplate + sinkTemplate;
    }
}
