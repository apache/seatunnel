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

import org.apache.commons.io.FileUtils;
import org.apache.seatunnel.core.sql.job.Executor;
import org.apache.seatunnel.core.sql.job.JobInfo;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class LocalSqlExample {

    public static final String TEST_RESOURCE_DIR = "/seatunnel-examples/seatunnel-sql-examples/src/main/resources/examples/";

    public static void main(String[] args) throws IOException {
        String configFile = getTestConfigFile("flink.sql.conf.template");
        String jobContent = FileUtils.readFileToString(new File(configFile), StandardCharsets.UTF_8);
        Executor.runJob(new JobInfo(jobContent));
    }

    public static String getTestConfigFile(String configFile) {
        return System.getProperty("user.dir") + TEST_RESOURCE_DIR + configFile;
    }
}
