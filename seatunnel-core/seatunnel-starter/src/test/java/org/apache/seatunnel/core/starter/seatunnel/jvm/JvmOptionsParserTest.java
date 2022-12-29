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

package org.apache.seatunnel.core.starter.seatunnel.jvm;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

class JvmOptionsParserTest {

    @SneakyThrows
    @Test
    void parse() {
        URI configPath = Resources.getResource("").toURI();
        final JvmOptionsParser parser = new JvmOptionsParser();
        List<String> jvmOptions = parser.readJvmOptionsFiles(Paths.get(configPath));
        String[] expectJvmOptions = {
            "-XX:+UseConcMarkSweepGC",
            "-XX:CMSInitiatingOccupancyFraction=75",
            "-XX:+UseCMSInitiatingOccupancyOnly",
            "-XX:+PrintGCDetails",
            "-XX:+PrintGCDateStamps",
            "-XX:+PrintTenuringDistribution",
            "-XX:+PrintGCApplicationStoppedTime",
            "-XX:+UseGCLogFileRotation",
            "-XX:NumberOfGCLogFiles=32",
            "-XX:GCLogFileSize=64m"};
        assertArrayEquals(expectJvmOptions, jvmOptions.toArray(), "Expected and actual jvmOptions is not equal");
    }
}
