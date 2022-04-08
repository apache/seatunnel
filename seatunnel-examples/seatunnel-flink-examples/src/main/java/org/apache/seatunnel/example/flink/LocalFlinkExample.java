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

package org.apache.seatunnel.example.flink;

import org.apache.seatunnel.Seatunnel;
import org.apache.seatunnel.command.FlinkCommandArgs;

public class LocalFlinkExample {

    public static final String TEST_RESOURCE_DIR = "/seatunnel-examples/seatunnel-flink-examples/src/main/resources/examples/";

    public static void main(String[] args) {
        String configFile = getTestConfigFile("fake_to_console.conf");
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        Seatunnel.run(flinkCommandArgs);
    }

    public static String getTestConfigFile(String configFile) {
        return System.getProperty("user.dir") + TEST_RESOURCE_DIR + configFile;
    }
}
