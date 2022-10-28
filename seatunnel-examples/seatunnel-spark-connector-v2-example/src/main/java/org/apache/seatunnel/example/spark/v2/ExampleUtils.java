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

package org.apache.seatunnel.example.spark.v2;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.spark.command.SparkCommandBuilder;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class ExampleUtils {

    public static void builder(String configurePath) throws FileNotFoundException, URISyntaxException, CommandException {
        String configFile = getTestConfigFile(configurePath);
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setConfigFile(configFile);
        sparkCommandArgs.setCheckConfig(false);
        sparkCommandArgs.setVariables(null);
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        Command<SparkCommandArgs> sparkCommand =
                new SparkCommandBuilder().buildCommand(sparkCommandArgs);
        Seatunnel.run(sparkCommand);
    }

    private static String getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelApiExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
