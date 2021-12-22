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
import java.util.HashMap;
import java.util.Map;

public class ExposeSparkConf {

    private static final String spark_executor_extraJavaOptions = "spark.executor.extraJavaOptions";
    private static final String spark_driver_prefix = "spark.driver.";

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        String variables = args[1];
        StringBuilder stringBuilder = new StringBuilder();
        Map<String, String> sparkConfMap = new HashMap<>();
        for (Map.Entry<String, ConfigValue> entry : appConfig.getConfig("spark").entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().unwrapped().toString();
            if (key.equals("spark.yarn.keytab") || key.equals("spark.yarn.principal")) {
                String argKey = key.substring(key.lastIndexOf(".") + 1); // keytab, principal
                String conf = String.format(" --%s %s", argKey, value);
                stringBuilder.append(conf);
            } else {
                String v = sparkConfMap.getOrDefault(key, null);
                if (StringUtils.isBlank(v)) {
                    sparkConfMap.put(key, value);
                } else {
                    sparkConfMap.put(key, v + " " + value);
                }
            }
        }


        if (!sparkConfMap.containsKey(spark_executor_extraJavaOptions)) {
            sparkConfMap.put(spark_executor_extraJavaOptions, variables);
        } else {
            sparkConfMap.put(spark_executor_extraJavaOptions,
                    sparkConfMap.get(spark_executor_extraJavaOptions) + " " + variables);
        }

        for (Map.Entry<String, String> c : sparkConfMap.entrySet()) {
            String v = addLogPropertiesIfNeeded(c.getKey(), c.getValue());
            String conf = String.format(" --conf \"%s=%s\"", c.getKey(), v);
            stringBuilder.append(conf);
        }

        String sparkDriverConf = exposeSparkDriverConf(appConfig, variables);

        System.out.print(sparkDriverConf + " " + stringBuilder.toString());
    }


    /**
     * In client mode, this config must not be set through the SparkConf directly in your application
     * eg. using --driver-java-options instead of spark.driver.extraJavaOptions
     * @param appConfig
     * @param variables
     * @return
     */
    private static String exposeSparkDriverConf(Config appConfig, String variables) {
        Config sparkConfig = appConfig.getConfig("spark");

        Map<String, String> sparkDriverMap = new HashMap<>();
        if (TypesafeConfigUtils.hasSubConfig(sparkConfig, spark_driver_prefix)) {

            Config sparkDriverConfig = TypesafeConfigUtils.extractSubConfig(sparkConfig, spark_driver_prefix, true);
            for (Map.Entry<String, ConfigValue> entry : sparkDriverConfig.entrySet()) {
                String key = entry.getKey();
                SparkDriverSettings settings = SparkDriverSettings.fromProperty(key);
                if (settings != null) {
                    sparkDriverMap.put(settings.option, entry.getValue().unwrapped().toString());

                }
            }
        }

        if (!sparkDriverMap.containsKey(SparkDriverSettings.DRIVER_JAVA_OPTIONS.option)) {
            sparkDriverMap.put(SparkDriverSettings.DRIVER_JAVA_OPTIONS.option, variables);
        } else {
            sparkDriverMap.put(SparkDriverSettings.DRIVER_JAVA_OPTIONS.option,
                    sparkDriverMap.get(SparkDriverSettings.DRIVER_JAVA_OPTIONS.option) + " " + variables);
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> c : sparkDriverMap.entrySet()) {
            String v = addLogPropertiesIfNeeded(c.getKey(), c.getValue());
            String conf = String.format(" %s=\"%s\" ", c.getKey(), v);
            stringBuilder.append(conf);
        }

        return stringBuilder.toString();

    }

    /**
     * if log4j.configuration is not specified, set default file path
     * @param key
     * @param value
     * @return
     */
    private static String addLogPropertiesIfNeeded(String key, String value) {
        if (key.equals(SparkDriverSettings.DRIVER_JAVA_OPTIONS.option)
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
