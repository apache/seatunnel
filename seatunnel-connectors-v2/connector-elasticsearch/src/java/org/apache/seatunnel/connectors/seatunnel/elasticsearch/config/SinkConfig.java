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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.BulkConfig;

public class SinkConfig {

    public static final String INDEX = "index";

    public static final String INDEX_TYPE = "index_type";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String HOSTS = "hosts";

    public static final String MAX_BATCH_SIZE = "max_batch_size";

    public static final String MAX_RETRY_SIZE = "max_retry_size";

    public static void setValue(org.apache.seatunnel.shade.com.typesafe.config.Config pluginConfig){
        if(pluginConfig.hasPath(MAX_BATCH_SIZE)){
            BulkConfig.MAX_BATCH_SIZE = pluginConfig.getInt(MAX_BATCH_SIZE);
        }
        if(pluginConfig.hasPath(MAX_RETRY_SIZE)){
            BulkConfig.MAX_RETRY_SIZE = pluginConfig.getInt(MAX_RETRY_SIZE);
        }
    }

}
