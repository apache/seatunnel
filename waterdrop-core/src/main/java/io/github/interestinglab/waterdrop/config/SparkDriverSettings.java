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

import java.util.*;

public enum SparkDriverSettings {

    DRIVER_MEMORY("spark.driver.memory", "--driver-memory"),
    DRIVER_JAVA_OPTIONS("spark.driver.extraJavaOptions", "--driver-java-options"),
    DRIVER_LIBRARY_PATH("spark.driver.extraLibraryPath", "--driver-library-path"),
    DRIVER_CLASS_PATH("spark.driver.extraClassPath", "--driver-class-path");


    private static final Map<String, SparkDriverSettings> driverSettings;

    static {
        Map<String, SparkDriverSettings> map = new HashMap<String, SparkDriverSettings>();
        for (SparkDriverSettings config : values())
            map.put(config.property, config);

        driverSettings = Collections.unmodifiableMap(map);
    }

    private final String property;
    public final String option;

    SparkDriverSettings(String property, String option) {
        this.property = property;
        this.option = option;
    }

    public static SparkDriverSettings fromProperty(String property) {
        return driverSettings.getOrDefault(property, null);
    }
}
