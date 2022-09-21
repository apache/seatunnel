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

package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

/**
 * @author Xuxiaotuan
 * @since 2022-09-15 18:25
 */
@Data
public class TiKVParameters implements Serializable {
    /**
     * PD server host
     */
    public String host;
    /**
     * PD server port
     */
    public Integer pdPort;
    /**
     * TiKV server addresses
     */
    public String pdAddresses;

    private String startKey;

    private String endKey;
    private String keyField;

    private String keysPattern;

    private Integer limit;

    private TiKVDataType tikvDataType;

    public String getPdAddresses() {
        return this.getHost() + ":" + this.getPdPort();
    }

    public void initConfig(Config config) {
        // set host
        this.host = config.getString(TiKVConfig.HOST);
        // set port
        this.pdPort = config.getInt(TiKVConfig.PD_PORT);
        // set key
        if (config.hasPath(TiKVConfig.KEY)) {
            this.keyField = config.getString(TiKVConfig.KEY);
        }
        // set keysPattern
        if (config.hasPath(TiKVConfig.KEY_PATTERN)) {
            this.keysPattern = config.getString(TiKVConfig.KEY_PATTERN);
        }
        // default 10000
        if (config.hasPath(TiKVConfig.LIMIT)) {
            this.limit = TiKVConfig.LIMIT_DEFAULT;
        }

        // default KEY
        this.tikvDataType = tikvDataType.getDataType(config.getString(TiKVConfig.DATA_TYPE));
    }

}
