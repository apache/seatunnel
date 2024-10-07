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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;

@Data
public class TelemetryLogsConfig implements Serializable {

    private boolean enabled =
            ServerConfigOptions.TELEMETRY_LOGS_SCHEDULED_DELETION_ENABLE.defaultValue();
    private long keepTime =
            ServerConfigOptions.TELEMETRY_LOGS_SCHEDULED_DELETION_KEEP_TIME.defaultValue();
    private String cron = ServerConfigOptions.TELEMETRY_LOGS_SCHEDULED_DELETION_CRON.defaultValue();
    private String path = ServerConfigOptions.TELEMETRY_LOGS_PATH.defaultValue();
    private String prefix = ServerConfigOptions.TELEMETRY_LOGS_PREFIX.defaultValue();
}
