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

package org.apache.seatunnel.core.base.command;

import org.apache.seatunnel.apis.base.command.CommandArgs;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.base.config.ApiType;
import org.apache.seatunnel.core.base.config.EngineType;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.util.Collections;
import java.util.List;

public abstract class AbstractCommandArgs implements CommandArgs {

    @Parameter(names = {"-c", "--config"},
            description = "Config file",
            required = true)
    private String configFile;

    @Parameter(names = {"-i", "--variable"},
            description = "variable substitution, such as -i city=beijing, or -i date=20190318")
    private List<String> variables = Collections.emptyList();

    @Parameter(names = {"-api", "--api-type"},
            converter = ApiTypeConverter.class,
            description = "Api type, engine or seatunnel")
    private ApiType apiType = ApiType.ENGINE_API;

    // todo: use command type enum
    @Parameter(names = {"-t", "--check"},
            description = "check config")
    private boolean checkConfig = false;

    @Parameter(names = {"-h", "--help"},
            help = true,
            description = "Show the usage message")
    private boolean help = false;

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

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public ApiType getApiType() {
        return apiType;
    }

    public void setApiType(ApiType apiType) {
        this.apiType = apiType;
    }

    public EngineType getEngineType() {
        throw new UnsupportedOperationException("abstract class CommandArgs not support this method");
    }

    public DeployMode getDeployMode() {
        throw new UnsupportedOperationException("abstract class CommandArgs not support this method");
    }

    /**
     * Used to convert the api type string to the enum value.
     */
    private static class ApiTypeConverter implements IStringConverter<ApiType> {

        /**
         * If the '-api' is not set, then will not go into this convert method.
         *
         * @param value input value set by '-api' or '--api-type'
         * @return api type enum value
         */
        @Override
        public ApiType convert(String value) {
            for (ApiType apiType : ApiType.values()) {
                if (apiType.getApiType().equalsIgnoreCase(value)) {
                    return apiType;
                }
            }
            throw new IllegalArgumentException(String.format("API type %s not supported", value));
        }
    }

}
