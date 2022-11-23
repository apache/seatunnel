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

package org.apache.seatunnel.api.common;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

/**
 * This exception will throw when {@link SeaTunnelPluginLifeCycle#prepare(Config)} failed.
 */
public class PrepareFailException extends SeaTunnelRuntimeException {

    public PrepareFailException(String pluginName, PluginType type, String message) {
        super(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, String.format("PluginName: %s, PluginType: %s, Message: %s",
                pluginName, type.getType(), message));
    }

    public PrepareFailException(String pluginName, PluginType type, String message, Throwable cause) {
        super(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, String.format("PluginName: %s, PluginType: %s, Message: %s",
                pluginName, type.getType(), message), cause);
    }
}
