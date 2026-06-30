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

package mongodb.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.NS_FIELD;

public class MongodbUtilsTest {

    @Test
    public void testBuildConnectionStringFromHostsKeepsLegacyFormat() {
        String connectionString =
                MongodbUtils.buildConnectionString(
                                "user", "password", "localhost:27017", "replicaSet=test")
                        .getConnectionString();

        Assertions.assertEquals(
                "mongodb://user:password@localhost:27017/?replicaSet=test", connectionString);
    }

    @Test
    public void testBuildConnectionStringKeepsSrvUri() {
        String srvUri =
                "mongodb+srv://user:password@cluster0.example.mongodb.net/test"
                        + "?retryWrites=true&w=majority";

        String connectionString =
                MongodbUtils.buildConnectionString(null, null, srvUri, null).getConnectionString();

        Assertions.assertEquals(srvUri, connectionString);
    }

    @Test
    public void testBuildConnectionStringAddsCredentialsAndOptionsToSrvUri() {
        String connectionString =
                MongodbUtils.buildConnectionString(
                                "user",
                                "password",
                                "mongodb+srv://cluster0.example.mongodb.net",
                                "retryWrites=true")
                        .getConnectionString();

        Assertions.assertEquals(
                "mongodb+srv://user:password@cluster0.example.mongodb.net/?retryWrites=true",
                connectionString);
    }

    @Test
    public void testPartitionMapUsesSrvNamespaceWithoutCredentialsOrOptions() {
        String hosts =
                "mongodb+srv://user:password@cluster0.example.mongodb.net/admin"
                        + "?retryWrites=true";

        Assertions.assertEquals(
                "mongodb+srv://cluster0.example.mongodb.net/inventory.products",
                MongodbRecordUtils.createPartitionMap(hosts, "inventory", "products")
                        .get(NS_FIELD));
        Assertions.assertEquals(
                "mongodb+srv://cluster0.example.mongodb.net/__mongodb_heartbeats",
                MongodbRecordUtils.createHeartbeatPartitionMap(hosts).get(NS_FIELD));
    }
}
