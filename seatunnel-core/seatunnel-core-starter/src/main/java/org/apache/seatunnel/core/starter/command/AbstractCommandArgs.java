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

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.config.EngineType;

import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.List;

public abstract class AbstractCommandArgs implements CommandArgs {

    @Parameter(names = {"-i", "--variable"},
        description = "variable substitution, such as -i city=beijing, or -i date=20190318")
    private List<String> variables = Collections.emptyList();

    // todo: use command type enum
    @Parameter(names = {"-t", "--check"},
            description = "check config")
    private boolean checkConfig = false;

    @Parameter(names = {"-n", "--name"},
            description = "application name")
    private String jobName = Constants.LOGO;

    @Parameter(names = {"-h", "--help"},
            help = true,
            description = "Show the usage message")
    private boolean help = false;

    /**
     * Undefined parameters parsed will be stored here as engine original command parameters.
     */
    private List<String> originalParameters;

    public List<String> getVariables() {
        return variables;
    }

    public void setVariables(List<String> variables) {
        this.variables = variables;
    }

    public boolean isCheckConfig() {
        return checkConfig;
    }

    public void setCheckConfig(boolean checkConfig) {
        this.checkConfig = checkConfig;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public List<String> getOriginalParameters() {
        return originalParameters;
    }

    public void setOriginalParameters(List<String> originalParameters) {
        this.originalParameters = originalParameters;
    }

    public EngineType getEngineType() {
        throw new UnsupportedOperationException("abstract class CommandArgs not support this method");
    }

    public DeployMode getDeployMode() {
        throw new UnsupportedOperationException("abstract class CommandArgs not support this method");
    }

    public String getConfigFile() {
        throw new UnsupportedOperationException("abstract class CommandArgs not support this method");
    }

    public void setConfigFile(String configFile) {
        throw new UnsupportedOperationException("abstract class CommandArgs not support this method");
    }

}
