/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.config;


import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExposeSparkConf {

    private static final String spark_driver_extraJavaOptions = "spark.driver.extraJavaOptions";
    private static final String spark_executor_extraJavaOptions = "spark.executor.extraJavaOptions";

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        String variables = args[1];
        StringBuilder stringBuilder = new StringBuilder();
        Map<String, String> sparkConfs = new LinkedHashMap<String, String>();
        for (Map.Entry<String, ConfigValue> entry : appConfig.getConfig("spark").entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().unwrapped().toString();
            if (key.equals("spark.yarn.keytab") || key.equals("spark.yarn.principal")) {
                String argKey = key.substring(key.lastIndexOf(".") + 1); // keytab, principal
                String conf = String.format(" --%s %s", argKey, value);
                stringBuilder.append(conf);
            } else {
                String v = sparkConfs.getOrDefault(key, null);
                if (StringUtils.isBlank(v)) {
                    sparkConfs.put(key, value);
                } else {
                    sparkConfs.put(key, v + " " + value);
                }
            }
        }


        if (!sparkConfs.containsKey(spark_driver_extraJavaOptions)) {
            sparkConfs.put(spark_driver_extraJavaOptions, variables);
        } else {
            sparkConfs.put(spark_driver_extraJavaOptions,
                    sparkConfs.get(spark_driver_extraJavaOptions) + " " + variables);
        }

        if (!sparkConfs.containsKey(spark_executor_extraJavaOptions)) {
            sparkConfs.put(spark_executor_extraJavaOptions, variables);
        } else {
            sparkConfs.put(spark_executor_extraJavaOptions,
                    sparkConfs.get(spark_executor_extraJavaOptions + " " + variables));
        }

        for (Map.Entry<String, String> c : sparkConfs.entrySet()) {
            String v = addLogPropertiesIfNeeded(c.getKey(), c.getValue());
            String conf = String.format(" --conf \"%s=%s\"", c.getKey(), v);
            stringBuilder.append(conf);
        }

        System.out.print(stringBuilder.toString());
    }

    /**
     * if log4j.configuration is not specified, set default file path
     * @param key
     * @param value
     * @return
     */
    private static String addLogPropertiesIfNeeded(String key, String value) {
        if (key.equals(spark_driver_extraJavaOptions)
                || key.equals(spark_executor_extraJavaOptions)) {
            if (!value.contains("-Dlog4j.configuration")) {
                return value + " " + logConfiguration();
            }
        }

        return value;
    }

    private static String runningJarPath() {
        try {
            URI jarUri = ExposeSparkConf.class.getProtectionDomain().getCodeSource().getLocation()
                .toURI();
            Path jarPath = Paths.get(jarUri);
            return jarPath.getParent().getParent().toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("failed to get work dir of seatunnel");
        }
    }

    private static String logConfiguration() {
        return String.format("-Dlog4j.configuration=file:%s/config/log4j.properties", runningJarPath());
    }
}
