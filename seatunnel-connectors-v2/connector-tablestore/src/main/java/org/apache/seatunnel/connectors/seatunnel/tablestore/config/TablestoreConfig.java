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

package org.apache.seatunnel.connectors.seatunnel.tablestore.config;

import java.io.Serializable;

public class TablestoreConfig implements Serializable {
    public static final String END_POINT = "end_point";
    public static final String INSTANCE_NAME = "instance_name";
    public static final String ACCESS_KEY_ID = "access_key_id";
    public static final String ACCESS_KEY_SECRET = "access_key_secret";
    public static final String TABLE = "table";
    public static final String BATCH_SIZE = "batch_size";
    public static final String DEFAULT_BATCH_INTERVAL_MS = "batch_interval_ms";
    public static final String PRIMARY_KEYS = "primary_keys";
}
