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

package org.seatunnel.connectors.cdc.base.option;
/**
 * Startup mode for the pulsar consumer, see {@link #CURSOR_STARTUP_MODE}.
 */
public enum StopMode {
    /**
     * Start from the latest cursor.
     */
    LATEST,
    /**
     * Start from user-supplied timestamp for each partition.
     */
    TIMESTAMP,
    /**
     * Start from user-supplied specific cursors for each partition.
     */
    SPECIFIC,
    NEVER;
}
