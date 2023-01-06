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
     * checkOptions method have a default implement to check whether `save_mode` parameter is in config.
     *
     * @param config config of Sink Connector
     * @return TableSaveMode TableSaveMode
     */
    default void checkOptions(Config config) {
        if (config.hasPath(SinkCommonOptions.DATA_SAVE_MODE)) {
            String tableSaveMode = config.getString(SinkCommonOptions.DATA_SAVE_MODE);
            DataSaveMode dataSaveMode = DataSaveMode.valueOf(tableSaveMode.toUpperCase(Locale.ROOT));
            if (!supportedDataSaveModeValues().contains(dataSaveMode)) {
                throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "This connector don't support save mode: " + dataSaveMode);
            }
        } else {
            throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                SinkCommonOptions.DATA_SAVE_MODE + " must in config");
        }
    }

    DataSaveMode getDataSaveModeUsed();

    /**
     * Return the DataSaveMode list supported by this connector
     * @return
     */
    List<DataSaveMode> supportedDataSaveModeValues();

    void handleSaveMode(DataSaveMode tableSaveMode);
}
