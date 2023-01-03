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

import java.util.Locale;

/**
 * The Sink Connectors which support TableSaveMode should implement this interface
 */
public interface SupportTableSaveMode {

    /**
     * We hope every sink connector use the same option name to config SaveMode, So I add checkOptions method to this interface.
     * checkOptions method have a default implement to check whether `table_save_mode` parameter is in config.
     *
     * @param config config of Sink Connector
     * @return
     */
    default void checkOptions(Config config) {
        if (config.hasPath(SinkCommonOptions.TABLE_SAVE_MODE.key())) {
            String tableSaveMode = config.getString(SinkCommonOptions.TABLE_SAVE_MODE.key());
            TableSaveMode.valueOf(tableSaveMode.toUpperCase(Locale.ROOT));
        } else {
            throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                SinkCommonOptions.TABLE_SAVE_MODE.key() + " must in config");
        }
    }

    void handleSaveMode(TableSaveMode tableSaveMode);
}
