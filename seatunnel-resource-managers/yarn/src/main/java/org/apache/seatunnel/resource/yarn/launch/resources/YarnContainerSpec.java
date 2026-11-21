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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.engine.common.config.EngineConfig.ClusterRole;
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerCommand;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.decorators.YarnStepDecorator;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

import lombok.Getter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** The AM or worker launch context and the resource capability YARN must allocate for it. */
@Getter
public final class YarnContainerSpec extends YarnResource<ContainerLaunchContext> {

    private final ClusterRole role;
    private final YarnResourceRequirements requirements;
    private final YarnLaunchParameters parameters;
    private final YarnContainerCommand command;

    public YarnContainerSpec(
            ClusterRole role,
            YarnResourceRequirements requirements,
            YarnLaunchParameters parameters,
            String entrypoint,
            List<String> arguments) {
        super(role.name(), Records.newRecord(ContainerLaunchContext.class));
        this.role = role;
        this.requirements = requirements;
        this.parameters = parameters;
        this.command =
                new YarnContainerCommand(
                        parameters.getLocalized().getHome(),
                        requirements.getMemoryMb(),
                        entrypoint,
                        arguments);
        getInternalResource().setLocalResources(parameters.getLocalized().getResources());
    }

    public boolean isMaster() {
        return role == ClusterRole.MASTER;
    }

    /**
     * Passes each step's result to the next, then renders the NodeManager command with all
     * contributed JVM options.
     *
     * @param decorators steps selected by the role's factory in launch order
     * @return the final spec containing the rendered command
     * @throws YarnClientException if a step fails to prepare launch data due to an I/O error
     */
    public YarnContainerSpec applyDecorators(List<? extends YarnStepDecorator<?>> decorators) {
        YarnContainerSpec spec = this;
        try {
            for (YarnStepDecorator<?> decorator : decorators) {
                spec = decorator.decorate(spec);
            }
        } catch (IOException failure) {
            throw new YarnClientException(
                    "Could not prepare YARN " + role + " container for " + parameters.getStaging(),
                    failure);
        }
        spec.applyCommand();
        return spec;
    }

    /** All JVM arguments must precede the entrypoint in the command passed to the NodeManager. */
    public void applyCommand() {
        getInternalResource().setCommands(Collections.singletonList(command.render()));
    }
}
