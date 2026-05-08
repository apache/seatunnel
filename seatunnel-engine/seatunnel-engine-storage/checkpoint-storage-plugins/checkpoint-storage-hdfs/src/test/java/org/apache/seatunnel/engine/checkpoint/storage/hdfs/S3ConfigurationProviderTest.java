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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.S3Configuration;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3ConfigurationProviderTest {

    @Test
    void testCredentialsProviderPropagatedToHadoopConf() {
        Map<String, String> config = new HashMap<>();
        config.put("s3.bucket", "s3a://test-bucket");
        config.put(
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        S3Configuration s3Config = new S3Configuration();
        Configuration hadoopConf = s3Config.buildConfiguration(config);

        assertEquals(
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                hadoopConf.get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    void testWebIdentityProviderClassAvailable() {
        // ClassNotFoundException indicates aws-java-sdk-bundle version is too old (< 1.12.x)
        assertDoesNotThrow(
                () -> Class.forName("com.amazonaws.auth.WebIdentityTokenCredentialsProvider"));
    }
}
