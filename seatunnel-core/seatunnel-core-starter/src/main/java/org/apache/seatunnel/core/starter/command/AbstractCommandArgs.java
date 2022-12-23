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

import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.List;

/**
 * Abstract class of {@link CommandArgs} implementation to save common configuration settings
 */
public abstract class AbstractCommandArgs implements CommandArgs {

    @Parameter(names = {"-c", "--config"},
            description = "Config file",
            required = true)
    protected String configFile;

    @Parameter(names = {"-i", "--variable"},
        description = "Variable substitution, such as -i city=beijing, or -i date=20190318")
    protected List<String> variables = Collections.emptyList();

    @Parameter(names = {"--check"},
            description = "Whether check config")
    protected boolean checkConfig = false;

    @Parameter(names = {"-n", "--name"},
            description = "SeaTunnel job name")
    protected String jobName = Constants.LOGO;

    @Parameter(names = {"-h", "--help"},
            help = true,
            description = "Show the usage message")
    protected boolean help = false;

    /**
     * Undefined parameters parsed will be stored here as engine original command parameters.
     */
    protected List<String> originalParameters;

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

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

    public DeployMode getDeployMode() {
        throw new UnsupportedOperationException("Abstract class does not support this operation");
    }
}
