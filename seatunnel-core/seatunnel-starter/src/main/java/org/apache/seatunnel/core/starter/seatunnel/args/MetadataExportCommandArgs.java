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
import org.apache.seatunnel.core.starter.seatunnel.command.MetadataExportCommand;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataExportCommandArgs extends CommandArgs {

    @Parameter(
            names = {"-o", "--output"},
            description =
                    "Output file path for the connector metadata JSON (default: connector_metadata.json)")
    private String outputPath = "connector_metadata.json";

    @Parameter(
            names = {"-pt", "--plugin-type"},
            description =
                    "Export only the specified plugin type: source, sink, transform. "
                            + "If not specified, exports all types.",
            converter = PluginTypeConverter.class)
    private PluginType pluginType;

    @Parameter(
            names = {"-p", "--plugin"},
            description =
                    "Export only the specified plugin name (e.g., Jdbc, Kafka). "
                            + "If not specified, exports all plugins.")
    private String pluginName;

    @Parameter(
            names = {"--pretty"},
            description = "Pretty-print the JSON output")
    private boolean prettyPrint = true;

    @Parameter(
            names = {"--stdout"},
            description = "Write JSON to stdout instead of a file")
    private boolean stdout = false;

    @Override
    public Command<?> buildCommand() {
        return new MetadataExportCommand(this);
    }

    public static class PluginTypeConverter implements IStringConverter<PluginType> {
        @Override
        public PluginType convert(String value) {
            try {
                return PluginType.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Plugin type must be one of: [source, sink, transform]");
            }
        }
    }
}
