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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;

/**
 * Verifies that the {@code -pt} plugin type argument is parsed independently of the JVM default
 * locale, through the command line parser and the metadata export converter.
 */
@ResourceLock("java.util.Locale")
public class ConnectorCheckCommandArgsTest {
    @Test
    public void testConnectorCheckCommandArgs() {
        String[] args = {"-l", "-pt", "source", "-o", "Paimon"};
        ConnectorCheckCommandArgs connectorCheckCommandArgs =
                CommandLineUtils.parse(
                        args, new ConnectorCheckCommandArgs(), "seatunnel-connector.sh", false);
        Assertions.assertTrue(connectorCheckCommandArgs.isListConnectors());
        Assertions.assertEquals(connectorCheckCommandArgs.getPluginType(), PluginType.SOURCE);
        Assertions.assertEquals(connectorCheckCommandArgs.getPluginIdentifier(), "Paimon");

        String[] illegalArgs = {"-l", "-pt", "**"};
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        CommandLineUtils.parse(
                                illegalArgs,
                                new ConnectorCheckCommandArgs(),
                                "seatunnel-connector.sh",
                                false));
    }

    @Test
    public void testPluginTypeArgumentIsLocaleIndependent() {
        Locale base = Locale.getDefault();
        Locale display = Locale.getDefault(Locale.Category.DISPLAY);
        Locale format = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            String[] args = {"-pt", "sink"};
            ConnectorCheckCommandArgs connectorCheckCommandArgs =
                    CommandLineUtils.parse(
                            args, new ConnectorCheckCommandArgs(), "seatunnel-connector.sh", false);
            Assertions.assertEquals(PluginType.SINK, connectorCheckCommandArgs.getPluginType());

            MetadataExportCommandArgs metadataExportCommandArgs = new MetadataExportCommandArgs();
            metadataExportCommandArgs.setPluginType(
                    new MetadataExportCommandArgs.PluginTypeConverter().convert("sink"));
            Assertions.assertEquals(PluginType.SINK, metadataExportCommandArgs.getPluginType());
        } finally {
            Locale.setDefault(base);
            Locale.setDefault(Locale.Category.DISPLAY, display);
            Locale.setDefault(Locale.Category.FORMAT, format);
        }
    }
}
