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

package org.apache.seatunnel.spark.clickhouse.sink

import org.apache.seatunnel.shade.com.typesafe.config.Config
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory

import org.scalatest.funsuite.AnyFunSuite

class ClickhouseFileTest extends AnyFunSuite {

  test("test for localizationEngine") {
    var originalEngine: String = "ReplicatedMergeTree";
    var replicatedMergeTreeDDL: String = "CREATE TABLE default.replicatedMergeTreeTable (`shard_key` Int32, `order_id` String, " +
      "`user_name` String, `user_id` String, `order_time` DateTime, `bi_dt` String)" +
      " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default/replicatedMergeTreeTable', " +
      "'{replica}') PARTITION BY bi_dt ORDER BY (order_time, user_id)" +
      " SETTINGS index_granularity = 8192";
    var localizationDDL = "CREATE TABLE default.replicatedMergeTreeTable (`shard_key` Int32, `order_id` String, " +
      "`user_name` String, `user_id` String, `order_time` DateTime, `bi_dt` String)" +
      " ENGINE = MergeTree() PARTITION BY bi_dt ORDER BY (order_time, user_id)" +
      " SETTINGS index_granularity = 8192";
    val table: Table = new Table(name = "test", database = "test",
      engine = originalEngine, createTableDDL = replicatedMergeTreeDDL,
      engineFull = "replicatedMergeTree", dataPaths = List[String]())
    assert(localizationDDL.equals(table.localizationEngine(originalEngine, replicatedMergeTreeDDL)))
  }

}
