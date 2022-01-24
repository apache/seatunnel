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

package org.apache.seatunnel.example.spark;

import static org.apache.seatunnel.utils.Engine.SPARK;

import org.apache.seatunnel.Seatunnel;
import org.apache.seatunnel.config.command.CommandLineArgs;

public class LocalSparkExample {

    public static final String TEST_RESOURCE_DIR = "/seatunnel-examples/seatunnel-spark-examples/src/main/resources/examples/";

    public static void main(String[] args) {
        String configFile = getTestConfigFile("spark.batch.conf.template");
        CommandLineArgs sparkArgs = new CommandLineArgs(configFile, false);
        Seatunnel.run(sparkArgs, SPARK);
    }

    public static String getTestConfigFile(String configFile) {
        return System.getProperty("user.dir") + TEST_RESOURCE_DIR + configFile;
    }
}
