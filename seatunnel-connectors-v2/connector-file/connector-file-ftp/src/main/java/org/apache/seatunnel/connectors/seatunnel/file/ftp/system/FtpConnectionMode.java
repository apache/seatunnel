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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.system;

/** Ftp connection mode enum. href="http://commons.apache.org/net/">Apache Commons Net</a>. */
public enum FtpConnectionMode {

    /** ACTIVE_LOCAL_DATA_CONNECTION_MODE */
    ACTIVE_LOCAL("active_local"),

    /** PASSIVE_LOCAL_DATA_CONNECTION_MODE */
    PASSIVE_LOCAL("passive_local");

    private final String mode;

    FtpConnectionMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static FtpConnectionMode fromMode(String mode) {
        for (FtpConnectionMode ftpConnectionModeEnum : FtpConnectionMode.values()) {
            if (ftpConnectionModeEnum.getMode().equals(mode.toLowerCase())) {
                return ftpConnectionModeEnum;
            }
        }
        throw new IllegalArgumentException("Unknown ftp connection mode: " + mode);
    }
}
