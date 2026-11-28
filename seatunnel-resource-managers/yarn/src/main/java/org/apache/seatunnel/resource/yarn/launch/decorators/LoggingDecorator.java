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

package org.apache.seatunnel.resource.yarn.launch.decorators;

import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerCommand;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;

import org.apache.hadoop.yarn.api.ApplicationConstants;

/** Routes the file appender into the YARN log directory and selects the localized Log4j file. */
public final class LoggingDecorator extends AbstractYarnStepDecorator {

    @Override
    public YarnContainerSpec decorate(YarnContainerSpec spec) {
        YarnLaunchParameters parameters = spec.getParameters();
        YarnContainerCommand command = spec.getCommand();
        command.addOption(YarnConstants.SYSPROP_HAZELCAST_LOGGING);
        command.addOption(
                YarnConstants.SYSPROP_LOGS_PATH
                        + YarnContainerCommand.quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR));
        command.addOption(YarnConstants.APPLICATION_LOG_FILE);
        command.addFileOption(
                YarnConstants.SYSPROP_LOG4J_CONFIG, parameters.path(YarnOptions.LOG4J_CONFIG_FILE));
        return spec;
    }
}
