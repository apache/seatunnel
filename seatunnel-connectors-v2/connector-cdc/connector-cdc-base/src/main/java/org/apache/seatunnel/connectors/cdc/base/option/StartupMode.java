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

package org.apache.seatunnel.connectors.cdc.base.option;

/**
 * Startup modes for the CDC Connectors, see {@link SourceOptions#STARTUP_MODE}.
 */
public enum StartupMode {
    /**
     * Startup from the earliest offset possible.
     */
    EARLIEST,
    /**
     * Startup from the latest offset.
     */
    LATEST,
    /**
     * Synchronize historical data at startup, and then synchronize incremental data.
     */
    INITIAL,
    /**
     * Start from user-supplied timestamp.
     */
    TIMESTAMP,
    /**
     * Startup from user-supplied specific offsets.
     */
    SPECIFIC
}
