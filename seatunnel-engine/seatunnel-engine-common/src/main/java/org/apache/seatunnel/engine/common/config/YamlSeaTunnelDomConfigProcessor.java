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
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import org.w3c.dom.Node;

public class YamlSeaTunnelDomConfigProcessor extends AbstractDomConfigProcessor {

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

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    private void parseEngineConfig(Node engineNode, SeaTunnelConfig config) {
        final EngineConfig engineConfig = config.getEngineConfig();
        for (Node node : childElements(engineNode)) {
            String name = cleanNodeName(node);
            switch (name) {
                case "backup-count":
                    engineConfig.setBackupCount(
                        getIntegerValue("backup-count", getTextContent(node))
                    );
                    break;
                default:
                    throw new AssertionError("Unrecognized element: " + name);
            }
        }
    }
}
