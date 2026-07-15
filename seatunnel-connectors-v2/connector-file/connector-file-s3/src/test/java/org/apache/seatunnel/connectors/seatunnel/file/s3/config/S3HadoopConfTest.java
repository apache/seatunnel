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

package org.apache.seatunnel.connectors.seatunnel.file.s3.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class S3HadoopConfTest {

    @Test
    void testPutS3SK() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertTrue(conf.getExtraOptions().containsKey("fs.s3n.awsAccessKeyId"));

        config.remove("access_key");
        conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertTrue(conf.getExtraOptions().containsKey("fs.s3n.awsAccessKeyId"));

        config.remove("secret_key");
        conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertFalse(conf.getExtraOptions().containsKey("fs.s3n.awsAccessKeyId"));
    }

    @Test
    void testDefaultCredentialsProvider() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                S3FileBaseOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER,
                conf.getExtraOptions().get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key()));
    }

    @Test
    void testCustomCredentialsProviderIsPassedThrough() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER,
                conf.getExtraOptions().get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key()));
    }

    @Test
    void testUnknownCredentialsProviderClassFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                "com.example.NonExistentCredentialsProvider");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(
                exception.getMessage().contains("com.example.NonExistentCredentialsProvider"));
    }
}
