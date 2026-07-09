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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.seatunnel.args.MetadataExportCommandArgs;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataExportCommandTest {

    @Test
    void shouldExportExtensionConditionDescription() throws Exception {
        Option<Integer> port =
                Options.key("port").intType().noDefaultValue().withDescription("Port number");
        ConditionExtension<Integer> portRangeExtension =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "must be between 1 and 65535";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value) {
                        return value != null && value >= 1 && value <= 65535;
                    }
                };
        OptionRule optionRule =
                OptionRule.builder()
                        .required(port, Conditions.extension(port, portRangeExtension))
                        .build();

        MetadataExportCommand command = new MetadataExportCommand(new MetadataExportCommandArgs());
        Method exportConnector =
                MetadataExportCommand.class.getDeclaredMethod(
                        "exportConnector",
                        PluginIdentifier.class,
                        OptionRule.class,
                        PluginType.class);
        exportConnector.setAccessible(true);

        ObjectNode connectorNode =
                (ObjectNode)
                        exportConnector.invoke(
                                command,
                                PluginIdentifier.of("seatunnel", "source", "ExtensionSource"),
                                optionRule,
                                PluginType.SOURCE);

        JsonNode valueConstraint = connectorNode.get("valueConstraints").get(0);
        assertTrue(
                valueConstraint.get("expression").asText().contains("must be between 1 and 65535"));

        JsonNode conditionTree = valueConstraint.get("conditionTree");
        assertEquals("port", conditionTree.get("key").asText());
        assertEquals("must be between 1 and 65535", conditionTree.get("expectValue").asText());
        assertEquals("extension", conditionTree.get("compareOperator").asText());
        assertEquals("EXTENSION", conditionTree.get("conditionOperator").asText());
        assertEquals("EXTENSION", conditionTree.get("conditionOperatorCategory").asText());
    }
}
