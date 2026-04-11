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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

/**
 * Performs static analysis validation on the SeaTunnel job configuration file. This command is
 * triggered by {@code --check} or {@code --dry-run=static} and validates:
 *
 * <ul>
 *   <li>Config file syntax (HOCON/YAML validity)
 *   <li>Plugin class loadability (connector JAR exists and class is loadable)
 *   <li>Required option presence and value type validation
 *   <li>DAG topology (at least one Source, at least one Sink, no cycles)
 *   <li>Transform SQL syntax parsing
 * </ul>
 *
 * <p>No network connections are made and no data is read or written. Cost: milliseconds, zero
 * external dependencies.
 */
@Slf4j
public class SeaTunnelConfValidateCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    public SeaTunnelConfValidateCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @Override
    public void execute() throws ConfigCheckException {
        Path configPath = FileUtils.getConfigPath(clientCommandArgs);

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(clientCommandArgs.getJobName());
            jobConfig.setJobContext(new JobContext());

            MultipleTableJobConfigParser parser =
                    new MultipleTableJobConfigParser(
                            configPath.toString(),
                            clientCommandArgs.getVariables(),
                            new IdGenerator(),
                            jobConfig,
                            true);

            // parse(null) uses local classloader.
            // This triggers all static validations:
            // 1. HOCON syntax parsing
            // 2. Plugin class loadability
            // 3. Required option presence check
            // 4. Option value type validation
            // 5. DAG topology validation
            // 6. Transform SQL syntax parsing
            // SaveMode execution is skipped because dryRun=true.
            parser.parse(null);
        } catch (Exception e) {
            throw new ConfigCheckException("Static analysis failed: " + e.getMessage(), e);
        }
    }
}
