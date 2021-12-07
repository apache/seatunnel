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
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.common.utils.StringTemplate;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.spark.SparkEnvironment;
import io.github.interestinglab.waterdrop.spark.batch.BaseSparkBatchSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.Unit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Elasticsearch extends BaseSparkBatchSink {

    public static final String ES_PREFIX = "es.";
    Map<String, String> esCfg = new HashMap();

    @Override
    public Unit output(Dataset<Row> df, SparkEnvironment env) {
        String index = StringTemplate.substitute(config.getString("index"), config.getString("index_time_format"));

        JavaEsSparkSQL.saveToEs(df, index + "/" + config.getString("index_type"), this.esCfg);
        return null;
    }

    @Override
    public CheckResult checkConfig() {
        if(config.hasPath("hosts") && config.getStringList("hosts").size() > 0){
            List<String> hosts = config.getStringList("hosts");
            // TODO CHECK hosts
          return   new CheckResult(true, "");
        }else {
            return new CheckResult(false, "please specify [hosts] as a non-empty string list");
        }

    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {
        final ImmutableMap<String, String> map = ImmutableMap.of("index", "waterdrop", "index_type", "_doc", "index_time_format", "yyyy.MM.dd");
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);

        config.entrySet().forEach(entry -> {
            String key = entry.getKey();

            if (key.startsWith(ES_PREFIX)) {
                String value = String.valueOf(entry.getValue().unwrapped());
                esCfg.put(key, value);
            }
        });

        esCfg.put("es.nodes", String.join(",", config.getStringList("hosts")));

        System.out.println("[INFO] Output ElasticSearch Params:");
        esCfg.forEach((key, value) -> System.out.println("[INFO] \t" + key + " = " + value));

    }
}
