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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig;

/**
 * bulk es config
 */
public class BulkConfig {
    /**
     * once bulk es include max document size
     * {@link SinkConfig#MAX_BATCH_SIZE}
     */
    public static int MAX_BATCH_SIZE = 10;

    /**
     * the max retry size of bulk es
     * {@link SinkConfig#MAX_RETRY_SIZE}
     */
    public static int MAX_RETRY_SIZE = 3;
}
