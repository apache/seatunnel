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
package io.github.interestinglab.waterdrop.spark.source;

import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.spark.SparkEnvironment;
import io.github.interestinglab.waterdrop.spark.batch.BaseSparkBatchSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Hive extends BaseSparkBatchSource {
    @Override
    public Dataset<Row> getData(SparkEnvironment env) {
        return env.getSparkSession().sql(config.getString("pre_sql"));
    }

    @Override
    public CheckResult checkConfig() {
        final boolean preSql = config.hasPath("pre_sql");
        if (preSql) {
            return new CheckResult(true, "");
        } else {
            return new CheckResult(false, "please specify [pre_sql]");
        }

    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {

    }
}
