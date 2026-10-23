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

package org.apache.seatunnel.edge.agent.starter.command.db;

import lombok.Getter;

@Getter
public enum DbSubcommand {
    INFO("info"),
    WAL_SUMMARY("wal-summary"),
    WAL_LIST("wal-list"),
    WAL_SHOW("wal-show"),
    POSITIONS("positions"),
    WAL_PURGE_DEAD("wal-purge-dead"),
    WAL_RETRY_DEAD("wal-retry-dead"),
    WAL_UNSTICK_SENDING("wal-unstick-sending"),
    WAL_PURGE_ACKED("wal-purge-acked");

    private final String cliName;

    DbSubcommand(String cliName) {
        this.cliName = cliName;
    }

    public static DbSubcommand fromCliName(String name) {
        if (name == null) {
            return null;
        }
        for (DbSubcommand subcommand : values()) {
            if (subcommand.cliName.equals(name)) {
                return subcommand;
            }
        }
        return null;
    }
}
