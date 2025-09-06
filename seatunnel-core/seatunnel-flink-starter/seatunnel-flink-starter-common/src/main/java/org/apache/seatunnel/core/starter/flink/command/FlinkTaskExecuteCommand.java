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

package org.apache.seatunnel.core.starter.flink.command;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import org.apache.seatunnel.api.metalake.MetalakeClient;
import org.apache.seatunnel.api.metalake.MetalakeClientFactory;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

@Slf4j
public class FlinkTaskExecuteCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkTaskExecuteCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile = FileUtils.getConfigPath(flinkCommandArgs);
        checkConfigExist(configFile);
        Config config = null;
        boolean metalakeEnabled =
                Boolean.parseBoolean(System.getenv().getOrDefault("METALAKE_ENABLED", "false"));
        if (metalakeEnabled) {
            config =
                    getMetalakeConfig(
                            ConfigBuilder.of(configFile, flinkCommandArgs.getVariables()));
        } else {
            config = ConfigBuilder.of(configFile, flinkCommandArgs.getVariables());
        }
        // if user specified job name using command line arguments, override config option
        if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
        }
        FlinkExecution seaTunnelTaskExecution = new FlinkExecution(config);
        try {
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            throw new CommandExecuteException("Flink job executed failed", e);
        }
    }

    private Config getMetalakeConfig(Config jobConfigTmp) {
        Config update = jobConfigTmp;
        String metalakeType = System.getenv("METALAKE_TYPE");
        String metalakeUrl = System.getenv("METALAKE_URL");

        MetalakeClient metalakeClient = MetalakeClientFactory.create(metalakeType, metalakeUrl);

        try {
            ConfigList sourceList = jobConfigTmp.getList("source");
            List<ConfigValue> newSourceList = new ArrayList<>(sourceList);

            for (int i = 0; i < sourceList.size(); i++) {
                ConfigObject sourceObj = (ConfigObject) sourceList.get(i);
                if (sourceObj.containsKey("sourceId")) {
                    ConfigObject tmp = sourceObj;
                    String sourceId = sourceObj.toConfig().getString("sourceId");
                    JsonNode metalakeJson = metalakeClient.getMetaInfo(sourceId);
                    for (Map.Entry<String, ConfigValue> entry : sourceObj.entrySet()) {
                        String subKey = entry.getKey();
                        ConfigValue value = entry.getValue();

                        if (value.valueType() == ConfigValueType.STRING) {
                            String strValue = (String) value.unwrapped();
                            if (strValue.startsWith("${") && strValue.endsWith("}")) {
                                String placeholder = strValue.substring(2, strValue.length() - 1);

                                if (metalakeJson.has(placeholder)) {
                                    String replaced = metalakeJson.get(placeholder).asText();
                                    tmp =
                                            tmp.withValue(
                                                    subKey,
                                                    ConfigValueFactory.fromAnyRef(replaced));
                                }
                            }
                        }
                    }
                    newSourceList.set(i, tmp);
                }
            }
            update = update.withValue("source", ConfigValueFactory.fromIterable(newSourceList));
        } catch (IOException e) {
            log.error("Fail to get MetaInfo, metalakeUrl: {}", metalakeUrl, e);
        }

        try {
            ConfigList sinkList = jobConfigTmp.getList("sink");
            List<ConfigValue> newSinkList = new ArrayList<>(sinkList);

            for (int i = 0; i < sinkList.size(); i++) {
                ConfigObject sinkObj = (ConfigObject) sinkList.get(i);
                if (sinkObj.containsKey("sourceId")) {
                    ConfigObject tmp = sinkObj;
                    String sourceId = sinkObj.toConfig().getString("sourceId");
                    JsonNode metalakeJson = metalakeClient.getMetaInfo(sourceId);
                    for (Map.Entry<String, ConfigValue> entry : sinkObj.entrySet()) {
                        String subKey = entry.getKey();
                        ConfigValue value = entry.getValue();

                        if (value.valueType() == ConfigValueType.STRING) {
                            String strValue = (String) value.unwrapped();
                            if (strValue.startsWith("${") && strValue.endsWith("}")) {
                                String placeholder = strValue.substring(2, strValue.length() - 1);

                                if (metalakeJson.has(placeholder)) {
                                    String replaced = metalakeJson.get(placeholder).asText();
                                    tmp =
                                            tmp.withValue(
                                                    subKey,
                                                    ConfigValueFactory.fromAnyRef(replaced));
                                }
                            }
                        }
                    }
                    newSinkList.set(i, tmp);
                }
            }
            update = update.withValue("sink", ConfigValueFactory.fromIterable(newSinkList));
        } catch (IOException e) {
            log.error("Fail to get MetaInfo, metalakeUrl: {}", metalakeUrl, e);
        }
        return update;
    }
}
