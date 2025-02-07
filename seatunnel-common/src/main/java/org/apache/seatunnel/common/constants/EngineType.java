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

package org.apache.seatunnel.common.constants;

/** Engine type enum */
public enum EngineType {
    SPARK2("spark", "seatunnel-spark-2-starter.jar", "start-seatunnel-spark-2-connector-v2.sh"),
    SPARK3("spark", "seatunnel-spark-3-starter.jar", "start-seatunnel-spark-3-connector-v2.sh"),
    FLINK13("flink", "seatunnel-flink-13-starter.jar", "start-seatunnel-flink-13-connector-v2.sh"),
    FLINK15("flink", "seatunnel-flink-15-starter.jar", "start-seatunnel-flink-15-connector-v2.sh"),
    SEATUNNEL("seatunnel", "seatunnel-starter.jar", "seatunnel.sh");

    private final String engine;
    private final String starterJarName;
    private final String starterShellName;

    EngineType(String engine, String starterJarName, String starterShellName) {
        this.engine = engine;
        this.starterJarName = starterJarName;
        this.starterShellName = starterShellName;
    }

    public String getEngine() {
        return engine;
    }

    public String getStarterJarName() {
        return starterJarName;
    }

    public String getStarterShellName() {
        return starterShellName;
    }
}
