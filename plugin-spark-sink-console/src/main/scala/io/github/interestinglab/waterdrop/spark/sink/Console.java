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
package io.github.interestinglab.waterdrop.spark.sink;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.spark.SparkEnvironment;
import io.github.interestinglab.waterdrop.spark.batch.BaseSparkBatchSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Int;
import scala.Unit;

import java.util.List;


public class Console extends BaseSparkBatchSink {
    @Override
    public Unit output(Dataset<Row> df, SparkEnvironment env) {
        int limit = config.getInt("limit");

        final String serializer = config.getString("serializer");

        switch (serializer) {
            case "plain":
                if (limit == -1) {
                    df.show(Int.MaxValue(), false);
                } else if (limit > 0) {
                    df.show(limit, false);
                }
                break;
            case "json":
                if (limit == -1) {
                    final List<String> rows = df.toJSON().takeAsList(Int.MaxValue());
                    for (String row : rows) {
                        System.out.println(row);
                    }
                } else if (limit > 0) {
                    final List<String> rows = df.toJSON().takeAsList(limit);
                    for (String row : rows) {
                        System.out.println(row);
                    }
                }
                break;
            case "schema":
                df.printSchema();
                break;
            default:
        }
        return null;

    }

    @Override
    public CheckResult checkConfig() {

        final boolean b = !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1);
        if (b) {
            return new CheckResult(true, "");
        } else {
            return new CheckResult(false, "please specify [limit] as Number[-1, " + Int.MaxValue() + "]");
        }

    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {

        Config defaultConfig = ConfigFactory.parseMap(
                ImmutableMap.of("limit", 100, "serializer", "plain")
        );
        config = config.withFallback(defaultConfig);
    }
}
