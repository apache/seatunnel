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

package org.apache.seatunnel.core.starter.command;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

@Slf4j
public class ConfEncryptCommand implements Command<AbstractCommandArgs> {

    private final AbstractCommandArgs abstractCommandArgs;

    public ConfEncryptCommand(AbstractCommandArgs abstractCommandArgs) {
        this.abstractCommandArgs = abstractCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        if (abstractCommandArgs.isDecrypt()) {
            log.warn(
                    "When both --decrypt and --encrypt are specified, only --encrypt will take effect");
        }
        String encryptConfigFile = abstractCommandArgs.getConfigFile();
        Path configPath = Paths.get(encryptConfigFile);
        checkConfigExist(configPath);
        Config config =
                ConfigFactory.parseFile(configPath.toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        if (abstractCommandArgs.getVariables() != null) {
            abstractCommandArgs.getVariables().stream()
                    .filter(Objects::nonNull)
                    .map(variable -> variable.split("=", 2))
                    .filter(pair -> pair.length == 2)
                    .forEach(pair -> System.setProperty(pair[0], pair[1]));
            config =
                    config.resolveWith(
                            ConfigFactory.systemProperties(),
                            ConfigResolveOptions.defaults().setAllowUnresolved(true));
        }
        Config encryptConfig = ConfigShadeUtils.encryptConfig(config);
        log.info(
                "Encrypt config: \n{}",
                encryptConfig
                        .root()
                        .render(ConfigRenderOptions.defaults().setOriginComments(false)));
    }
}
