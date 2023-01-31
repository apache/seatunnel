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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;
import java.util.Locale;

/**
 * The Sink Connectors which support data SaveMode should implement this interface
 */
public interface SupportDataSaveMode {

    /**
     * We hope every sink connector use the same option name to config SaveMode, So I add checkOptions method to this interface.
     * checkOptions method have a default implement to check whether `save_mode` parameter is in option rule.
     *
     * @param config config of sink Connector
     */
    default void checkOptions(Config config, OptionRule optionRule) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        List<RequiredOption> requiredOptions = optionRule.getRequiredOptions();
        Option saveModeOption = null;
        for (RequiredOption requiredOption : requiredOptions) {
            for (Option option : requiredOption.getOptions()) {
                if (option.key().equals(SinkCommonOptions.DATA_SAVE_MODE)) {
                    saveModeOption = option;
                    break;
                }
            }
            if (saveModeOption != null) {
                break;
            }
        }

        if (saveModeOption == null) {
            for (Option option : optionRule.getOptionalOptions()) {
                if (option.key().equals(SinkCommonOptions.DATA_SAVE_MODE)) {
                    saveModeOption = option;
                    break;
                }
            }
        }
        if (saveModeOption == null) {
            throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "This connector must have a option named save_mode");
        }

        String tableSaveMode = readonlyConfig.get(saveModeOption).toString();
        DataSaveMode dataSaveMode = DataSaveMode.valueOf(tableSaveMode.toUpperCase(Locale.ROOT));
        if (!supportedDataSaveModeValues().contains(dataSaveMode)) {
            throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "This connector don't support save mode: " + dataSaveMode);
        }
    }

    /**
     * Get the {@link DataSaveMode} that the user configured
     * @return DataSaveMode
     */
    DataSaveMode getDataSaveMode();

    /**
     * Return the {@link DataSaveMode} list supported by this connector
     * @return the list of supported data save modes
     */
    List<DataSaveMode> supportedDataSaveModeValues();

    /**
     * The implementation of specific logic according to different {@link DataSaveMode}
     * @param saveMode data save mode
     */
    void handleSaveMode(DataSaveMode saveMode);
}
