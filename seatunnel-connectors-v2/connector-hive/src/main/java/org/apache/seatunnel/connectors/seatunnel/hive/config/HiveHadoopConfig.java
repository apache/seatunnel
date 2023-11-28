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

package org.apache.seatunnel.connectors.seatunnel.hive.config;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

public class HiveHadoopConfig extends HadoopConf {

    private final String hiveMetaStoreUris;
    private final String hiveSitePath;

    public HiveHadoopConfig(String hdfsNameKey, String hiveMetaStoreUris, String hiveSitePath) {
        super(hdfsNameKey);
        this.hiveMetaStoreUris = hiveMetaStoreUris;
        this.hiveSitePath = hiveSitePath;
    }

    public String getHiveMetaStoreUris() {
        return hiveMetaStoreUris;
    }

    public String getHiveSitePath() {
        return hiveSitePath;
    }
}
