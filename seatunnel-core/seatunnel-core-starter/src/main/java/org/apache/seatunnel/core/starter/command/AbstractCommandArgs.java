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
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;

/** Abstract class of {@link CommandArgs} implementation to save common configuration settings */
@EqualsAndHashCode(callSuper = true)
@Data
public abstract class AbstractCommandArgs extends CommandArgs {

    /** config file path */
    @Parameter(
            names = {"-c", "--config"},
            description = "Config file")
    protected String configFile;

    /** user-defined parameters */
    @Parameter(
            names = {"-i", "--variable"},
            splitter = ParameterSplitter.class,
            description = "Variable substitution, such as -i city=beijing, or -i date=20190318")
    protected List<String> variables = Collections.emptyList();

    /** check config flag */
    @Parameter(
            names = {"--check"},
            description = "Whether check config")
    protected boolean checkConfig = false;

    /** SeaTunnel job name */
    @Parameter(
            names = {"-n", "--name"},
            description = "SeaTunnel job name")
    protected String jobName = Constants.LOGO;

    @Parameter(
            names = {"--encrypt"},
            description =
                    "Encrypt config file, when both --decrypt and --encrypt are specified, only --encrypt will take effect")
    protected boolean encrypt = false;

    @Parameter(
            names = {"--decrypt"},
            description =
                    "Decrypt config file, When both --decrypt and --encrypt are specified, only --encrypt will take effect")
    protected boolean decrypt = false;

    public abstract DeployMode getDeployMode();
}
