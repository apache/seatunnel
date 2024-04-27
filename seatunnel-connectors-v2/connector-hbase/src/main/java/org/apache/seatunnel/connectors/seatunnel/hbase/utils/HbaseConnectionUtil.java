/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.utils;

import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnectionUtil {
    public static Connection getHbaseConnection(HbaseParameters hbaseParameters) {
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", hbaseParameters.getZookeeperQuorum());
        if (hbaseParameters.getHbaseExtraConfig() != null) {
            hbaseParameters.getHbaseExtraConfig().forEach(hbaseConfiguration::set);
        }
        // initialize hbase connection
        try {
            Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
            return connection;
        } catch (IOException e) {
            String errorMsg = "Build Hbase connection failed.";
            throw new HbaseConnectorException(HbaseConnectorErrorCode.CONNECTION_FAILED, errorMsg);
        }
    }
}
