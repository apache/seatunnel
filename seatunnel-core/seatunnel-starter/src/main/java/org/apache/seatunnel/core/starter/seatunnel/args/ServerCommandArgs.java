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

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.CommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.command.ServerExecuteCommand;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ServerCommandArgs extends CommandArgs {
    @Parameter(
            names = {"-cn", "--cluster"},
            description = "The name of cluster")
    private String clusterName;

    @Parameter(
            names = {"-d", "--daemon"},
            description = "The cluster daemon mode")
    private boolean daemonMode = false;

    @Parameter(
            names = {"-r", "--role"},
            description =
                    "The cluster node role, default is master_and_worker, support master, worker, master_and_worker")
    private String clusterRole;

    @Override
    public Command<?> buildCommand() {
        return new ServerExecuteCommand(this);
    }
}
