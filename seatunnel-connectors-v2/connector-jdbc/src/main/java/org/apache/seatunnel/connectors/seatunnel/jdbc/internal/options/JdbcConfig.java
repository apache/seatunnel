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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options;

import java.io.Serializable;

public class JdbcConfig implements Serializable {

    public static final String URL = "url";

    public static final String DRIVER = "driver";

    public static final String CONNECTION_CHECK_TIMEOUT_SEC = "connection_check_timeout_sec";

    public static final String MAX_RETRIES = "max_retries";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    public static final String QUERY = "query";

    public static final String PARALLELISM = "parallelism";


    public static final String BATCH_SIZE = "batch_size";

    public static final String BATCH_INTERVAL_MS = "batch_interval_ms";


    public static final String IS_EXACTLY_ONCE = "is_exactly_once";

    public static final String XA_DATA_SOURCE_CLASS_NAME = "xa_data_source_class_name";


    public static final String MAX_COMMIT_ATTEMPTS = "max_commit_attempts";

    public static final String TRANSACTION_TIMEOUT_SEC = "transaction_timeout_sec";

}
