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

package org.apache.seatunnel.core.starter.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class SparkStarterTest {

    @Test
    public void testGetSparkConf() throws URISyntaxException, FileNotFoundException {
        URI uri = ClassLoader.getSystemResource("spark_application.conf").toURI();
        String file = new File(uri).toString();
        Map<String, String> sparkConf = SparkStarter.getSparkConf(file);
        assertEquals("SeaTunnel", sparkConf.get("job.name"));
        assertEquals("1", sparkConf.get("spark.executor.cores"));
    }
}
