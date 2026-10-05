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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.seatunnel.args.MetadataExportCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Entry point for the connector metadata export tool.
 *
 * <p>Exports all connector option rules as structured JSON via runtime reflection. Uses the same
 * PluginDiscovery + factory.optionRule() mechanism as SeaTunnel Web — output is 100% accurate.
 *
 * <p>Usage: seatunnel-metadata-export.sh [-o output.json] [-pt source|sink] [-p Jdbc] [--stdout]
 */
@Slf4j
public class SeaTunnelMetadataExporter {
    private static final String SHELL_NAME = "seatunnel-metadata-export.sh";

    public static void main(String[] args) {
        MetadataExportCommandArgs commandArgs =
                CommandLineUtils.parse(args, new MetadataExportCommandArgs(), SHELL_NAME, true);
        SeaTunnel.run(commandArgs.buildCommand());
    }
}
