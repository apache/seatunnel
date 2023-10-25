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
    // Will drop table in MySQL, Will drop path for File Connector.
    DROP_SCHEMA,

    // Only drop the data in MySQL, Only drop the files in the path for File Connector.
    KEEP_SCHEMA_DROP_DATA,

    // Keep the table and data and continue to write data to the existing table for MySQL. Keep the
    // path and files in the path, create new files in the path.
    KEEP_SCHEMA_AND_DATA,

    // The connector provides custom processing methods, such as running user provided SQL or shell
    // scripts, etc
    CUSTOM_PROCESSING,

    // Throw error when table is exists for MySQL. Throw error when path is exists.
    ERROR_WHEN_EXISTS
}
