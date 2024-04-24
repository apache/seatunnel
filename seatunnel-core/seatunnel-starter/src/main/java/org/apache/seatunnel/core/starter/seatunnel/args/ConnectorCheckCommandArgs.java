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
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.CommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.command.ConnectorCheckCommand;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ConnectorCheckCommandArgs extends CommandArgs {
    @Parameter(
            names = {"-l", "--list"},
            description = "List all supported plugins(sources, sinks, transforms)")
    private boolean listConnectors = false;

    @Parameter(
            names = {"-o", "--option-rule"},
            description =
                    "Get option rule of the plugin by the plugin identifier(connector name or transform name)")
    private String pluginIdentifier;

    @Parameter(
            names = {"-pt", "--plugin-type"},
            description = "SeaTunnel plugin type, support [source, sink, transform]",
            converter = SeaTunnelPluginTypeConverter.class)
    private PluginType pluginType;

    @Override
    public Command<?> buildCommand() {
        return new ConnectorCheckCommand(this);
    }

    public static class SeaTunnelPluginTypeConverter implements IStringConverter<PluginType> {
        @Override
        public PluginType convert(String value) {
            try {
                return PluginType.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "The plugin type of seaTunnel only "
                                + "support these options: [source, transform, sink]");
            }
        }
    }
}
