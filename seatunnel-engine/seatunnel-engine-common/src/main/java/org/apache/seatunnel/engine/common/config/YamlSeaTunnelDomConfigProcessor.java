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

package org.apache.seatunnel.engine.common.config;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;

import org.apache.seatunnel.engine.common.config.server.ServerConfigName;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Node;

public class YamlSeaTunnelDomConfigProcessor extends AbstractDomConfigProcessor {

    private static final ILogger LOGGER = Logger.getLogger(YamlSeaTunnelDomConfigProcessor.class);

    private final SeaTunnelConfig config;

    YamlSeaTunnelDomConfigProcessor(boolean domLevel3, SeaTunnelConfig config) {
        super(domLevel3);
        this.config = config;
    }

    @Override
    public void buildConfig(Node rootNode) {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException(
                    "Duplicate '" + nodeName + "' definition found in the configuration.");
            }
            if (handleNode(node, nodeName)) {
                continue;
            }
            if (!SeaTunnelConfigSections.canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private boolean handleNode(Node node, String name) {
        if (SeaTunnelConfigSections.ENGINE.isEqual(name)) {
            parseEngineConfig(node, config);
        } else {
            return true;
        }
        return false;
    }

    private SlotServiceConfig parseSlotServiceConfig(Node slotServiceNode) {
        SlotServiceConfig slotServiceConfig = new SlotServiceConfig();
        for (Node node : childElements(slotServiceNode)) {
            String name = cleanNodeName(node);
            switch (name) {
                case ServerConfigName.DYNAMIC_SLOT:
                    slotServiceConfig.setDynamicSlot(getBooleanValue(getTextContent(node)));
                    break;
                case ServerConfigName.SLOT_NUM:
                    slotServiceConfig.setSlotNum(getIntegerValue(ServerConfigName.SLOT_NUM, getTextContent(node)));
                    break;
                default:
                    LOGGER.warning("Unrecognized element: " + name);
            }
        }
        return slotServiceConfig;
    }

    private void parseEngineConfig(Node engineNode, SeaTunnelConfig config) {
        final EngineConfig engineConfig = config.getEngineConfig();
        for (Node node : childElements(engineNode)) {
            String name = cleanNodeName(node);
            switch (name) {
                case ServerConfigName.BACKUP_COUNT:
                    engineConfig.setBackupCount(
                        getIntegerValue(ServerConfigName.BACKUP_COUNT, getTextContent(node))
                    );
                    break;
                case ServerConfigName.PRINT_EXECUTION_INFO_INTERVAL:
                    engineConfig.setPrintExecutionInfoInterval(getIntegerValue(ServerConfigName.PRINT_EXECUTION_INFO_INTERVAL,
                        getTextContent(node)));
                    break;
                case ServerConfigName.SLOT_SERVICE:
                    engineConfig.setSlotServiceConfig(parseSlotServiceConfig(node));
                    break;
                default:
                    LOGGER.warning("Unrecognized element: " + name);
            }
        }
    }
}
