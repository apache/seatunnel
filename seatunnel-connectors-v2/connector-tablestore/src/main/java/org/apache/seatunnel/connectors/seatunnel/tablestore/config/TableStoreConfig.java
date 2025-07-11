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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class TableStoreConfig implements Serializable {

    private String endpoint;

    private String instanceName;

    private String accessKeyId;

    private String accessKeySecret;

    private String table;

    private List<String> primaryKeys;

    public int batchSize;

    public TableStoreConfig() {}

    public TableStoreConfig(ReadonlyConfig config) {
        this.endpoint = config.get(TableStoreCommonOptions.END_POINT);
        this.instanceName = config.get(TableStoreCommonOptions.INSTANCE_NAME);
        this.accessKeyId = config.get(TableStoreCommonOptions.ACCESS_KEY_ID);
        this.accessKeySecret = config.get(TableStoreCommonOptions.ACCESS_KEY_SECRET);
        this.table = config.get(TableStoreCommonOptions.TABLE);
        this.primaryKeys = config.get(TableStoreCommonOptions.PRIMARY_KEYS);
        this.batchSize = config.get(TableStoreSinkOptions.BATCH_SIZE);
    }
}
