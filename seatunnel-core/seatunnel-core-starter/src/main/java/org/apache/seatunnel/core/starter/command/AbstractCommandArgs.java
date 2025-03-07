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
import org.apache.seatunnel.core.starter.enums.CryptoMode;

import com.beust.jcommander.IStringConverter;
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
            description =
                    "Variable substitution, such as -i city=beijing, or -i date=20190318."
                            + "We use ',' as separator, when inside \"\", ',' are treated as normal characters instead of delimiters."
                            + " For example, -i city=\"beijing,shanghai\". If you want to use dynamic parameters,"
                            + " you can use the following format: -i date=$(date +\"%Y%m%d\").")
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
                    "Enable encryption. If set, encryption is applied with default rule unless --crypto-mode is specified.",
            arity = 0)
    protected boolean encrypt = false;

    @Parameter(
            names = {"--decrypt"},
            description =
                    "Enable decryption. If set, decryption is applied with default rule unless --crypto-mode is specified.",
            arity = 0)
    protected boolean decrypt = false;

    @Parameter(
            names = {"--crypto-mode"},
            description =
                    "Encryption/Decryption mode: 'default' or 'legacy'. When provided, the specified mode is applied.",
            converter = EncryptionModeConverter.class)
    protected CryptoMode cryptoMode = CryptoMode.DEFAULT;

    public abstract DeployMode getDeployMode();

    /** Custom converter for --encrypt and --decrypt parameters. */
    public static class EncryptionModeConverter implements IStringConverter<CryptoMode> {
        @Override
        public CryptoMode convert(String value) {
            // If no value is provided, treat it as DEFAULT
            if (value == null || value.isEmpty()) {
                return CryptoMode.DEFAULT;
            }
            // Otherwise, parse the value as a string and convert to EncryptionMode
            return CryptoMode.fromValue(value);
        }
    }
}
