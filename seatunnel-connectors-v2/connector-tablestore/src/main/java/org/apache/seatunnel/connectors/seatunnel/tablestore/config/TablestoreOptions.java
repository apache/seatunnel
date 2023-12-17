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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.BATCH_SIZE;

@Data
@AllArgsConstructor
public class TablestoreOptions implements Serializable {

    private String endpoint;

    private String instanceName;

    private String accessKeyId;

    private String accessKeySecret;

    private String table;

    private List<String> primaryKeys;

    public int batchSize = Integer.parseInt(BATCH_SIZE.defaultValue());

    public TablestoreOptions(Config config) {
        this.endpoint = config.getString(TablestoreConfig.END_POINT.key());
        this.instanceName = config.getString(TablestoreConfig.INSTANCE_NAME.key());
        this.accessKeyId = config.getString(TablestoreConfig.ACCESS_KEY_ID.key());
        this.accessKeySecret = config.getString(TablestoreConfig.ACCESS_KEY_SECRET.key());
        this.table = config.getString(TablestoreConfig.TABLE.key());
        this.primaryKeys = config.getStringList(TablestoreConfig.PRIMARY_KEYS.key());

        if (config.hasPath(BATCH_SIZE.key())) {
            this.batchSize = config.getInt(BATCH_SIZE.key());
        }
    }
}
