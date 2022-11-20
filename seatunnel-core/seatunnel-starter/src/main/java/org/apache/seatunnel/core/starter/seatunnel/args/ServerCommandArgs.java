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

import org.apache.seatunnel.core.starter.command.CommandArgs;

import com.beust.jcommander.Parameter;

import java.util.List;

public class ServerCommandArgs implements CommandArgs {

    /**
     * Undefined parameters parsed will be stored here as seatunnel engine command parameters.
     */
    private List<String> seatunnelParams;

    @Parameter(names = {"-cn", "--cluster"},
        description = "The name of cluster")
    private String clusterName = "seatunnel_default_cluster";

    @Parameter(names = {"-h", "--help"},
        help = true,
        description = "Show the usage message")
    private boolean help = false;

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getSeatunnelParams() {
        return seatunnelParams;
    }

    public void setSeatunnelParams(List<String> seatunnelParams) {
        this.seatunnelParams = seatunnelParams;
    }
}
