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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import lombok.NonNull;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class HiveMetaStoreProxy {

    private HiveMetaStoreClient hiveMetaStoreClient;

    public HiveMetaStoreProxy(@NonNull String uris) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", uris);
        try {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return hiveMetaStoreClient.getTable(dbName, tableName);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("checkstyle:UnnecessaryParentheses")
    public static void main(String[] args) {
        HiveMetaStoreProxy hi = new HiveMetaStoreProxy("thrift://49.7.112.201:9083");
        HiveMetaStoreClient hiveMetaStoreClient1 = hi.getHiveMetaStoreClient();
        try {
            Table table = hi.getTable("default", "test1");
            Partition part = new Partition();
            part.setDbName(table.getDbName());
            part.setTableName(table.getTableName());
            part.setValues(Arrays.stream((new String[]{"6"})).collect(Collectors.toList()));
            part.setParameters(new HashMap<>());
            part.setSd(table.getSd().deepCopy());
            part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
            part.getSd().setLocation(table.getSd().getLocation() + "/ds=6");
            try {
                hiveMetaStoreClient1.add_partition(part);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HiveMetaStoreClient getHiveMetaStoreClient() {
        return hiveMetaStoreClient;
    }
}
