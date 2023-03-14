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

package org.apache.seatunnel.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.config.utils.FileUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;

public class JsonFormatTest {

    @Test
    public void testJsonFormat() throws URISyntaxException {

        Config json =
                ConfigFactory.parseFile(FileUtils.getFileFromResources("/json/spark.batch.json"))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));

        Config config =
                ConfigFactory.parseFile(FileUtils.getFileFromResources("/json/spark.batch.conf"))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));

        Assertions.assertEquals(config.atPath("transform"), json.atPath("transform"));
        Assertions.assertEquals(config.atPath("sink"), json.atPath("sink"));
        Assertions.assertEquals(config.atPath("source"), json.atPath("source"));
        Assertions.assertEquals(config.atPath("env"), json.atPath("env"));
    }
}
