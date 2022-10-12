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

package org.apache.seatunnel.engine.common.config;

import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;

import lombok.Data;

@Data
@SuppressWarnings("checkstyle:MagicNumber")
public class EngineConfig {
    private int backupCount = ServerConfigOptions.BACKUP_COUNT.defaultValue();
    private int printExecutionInfoInterval = ServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL.defaultValue();

    private SlotServiceConfig slotServiceConfig = ServerConfigOptions.SLOT_SERVICE.defaultValue();

    private CheckpointConfig checkpointConfig = ServerConfigOptions.CHECKPOINT.defaultValue();

    public void setBackupCount(int newBackupCount) {
        checkBackupCount(newBackupCount, 0);
        this.backupCount = newBackupCount;
    }

    public void setPrintExecutionInfoInterval(int printExecutionInfoInterval) {
        checkPositive(printExecutionInfoInterval, ServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL + " must be > 0");
        this.printExecutionInfoInterval = printExecutionInfoInterval;
    }

}
