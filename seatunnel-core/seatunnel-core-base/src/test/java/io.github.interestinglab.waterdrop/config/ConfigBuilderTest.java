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

package io.github.interestinglab.waterdrop.config;

import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

import static io.github.interestinglab.waterdrop.utils.Engine.SPARK;

public class ConfigBuilderTest {

    @Test
    public void defineVariableTest() {
        String configFile = System.getProperty("user.dir") + "/src/test/resources/spark.batch.test.conf";
        HashMap variableMap = new HashMap();
        Tuple2<String, String> date = new Tuple2("date", "20210101");
        variableMap = variableMap.$plus(date);
        ConfigBuilder configBuilder = new ConfigBuilder(configFile, SPARK, variableMap);
        Config config = configBuilder.getEnvConfigs();
        Assert.assertTrue(config.getString("spark.app.name").contains("20210101"));
        String resText = configBuilder.replaceVariable(configFile, variableMap);
        Assert.assertTrue(resText.contains("20210101"));

        // negative case
        HashMap emptyVariableMap = new HashMap();
        ConfigBuilder configBuilder1 = new ConfigBuilder(configFile, SPARK, emptyVariableMap);
        Config config1 = configBuilder1.getEnvConfigs();
        Assert.assertTrue(!config1.getString("spark.app.name").contains("20210101"));
        resText = configBuilder.replaceVariable(configFile, emptyVariableMap);
        Assert.assertTrue(!resText.contains("20210101"));
    }
}
