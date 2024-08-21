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

/**
 * The SaveMode for the Sink connectors that use table or other table structures to organize data
 */
public enum DataSaveMode {

    // Preserve database structure and delete data
    DROP_DATA,

    // Preserve database structure, preserve data
    APPEND_DATA,

    // User defined processing
    CUSTOM_PROCESSING,

    // When there exist data, an error will be reported
    ERROR_WHEN_DATA_EXISTS
}
