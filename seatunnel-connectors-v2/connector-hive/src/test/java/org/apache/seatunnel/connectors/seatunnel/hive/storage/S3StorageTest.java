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

package org.apache.seatunnel.connectors.seatunnel.hive.storage;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3ConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOnS3Conf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;

public class S3StorageTest {

    private static final ReadonlyConfig S3A =
            ReadonlyConfig.fromMap(
                    new HashMap<String, Object>() {
                        {
                            put(
                                    "hive.hadoop.conf",
                                    new HashMap<String, String>() {
                                        {
                                            put(S3ConfigOptions.S3_BUCKET.key(), "s3a://my_bucket");
                                            put(
                                                    S3ConfigOptions.S3A_AWS_CREDENTIALS_PROVIDER
                                                            .key(),
                                                    "provider");
                                            put(
                                                    S3ConfigOptions.FS_S3A_ENDPOINT.key(),
                                                    "http://s3.ap-northeast-1.amazonaws.com");
                                        }
                                    });
                        }
                    });

    private static final ReadonlyConfig S3 =
            ReadonlyConfig.fromMap(
                    new HashMap<String, Object>() {
                        {
                            put(
                                    "hive.hadoop.conf",
                                    new HashMap<String, String>() {
                                        {
                                            put(S3ConfigOptions.S3_BUCKET.key(), "s3://my_bucket");
                                            put(
                                                    S3ConfigOptions.S3A_AWS_CREDENTIALS_PROVIDER
                                                            .key(),
                                                    "testProvider");
                                            put(S3ConfigOptions.FS_S3A_ENDPOINT.key(), "test");
                                        }
                                    });
                        }
                    });

    @Test
    void fillBucketInHadoopConf() {
        S3Storage s3Storage = new S3Storage();
        HadoopConf s3aConf = s3Storage.buildHadoopConfWithReadOnlyConfig(S3A);
        assertHadoopConfForS3a(s3aConf);

        HadoopConf s3Conf = s3Storage.buildHadoopConfWithReadOnlyConfig(S3);
        Assertions.assertTrue(s3Conf instanceof HiveOnS3Conf);
        Assertions.assertEquals(s3Conf.getSchema(), "s3");
        Assertions.assertEquals(
                s3Conf.getFsHdfsImpl(), "com.amazon.ws.emr.hadoop.fs.EmrFileSystem");
    }

    @Test
    void fillBucketInHadoopConfPath() throws URISyntaxException {
        URL resource = S3StorageTest.class.getResource("/s3");
        String filePath = Paths.get(resource.toURI()).toString();
        HashMap<String, Object> map = new HashMap<>();
        map.put("hive.hadoop.conf-path", filePath);
        map.putAll(S3A.toMap());
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(map);
        S3Storage s3Storage = new S3Storage();
        HadoopConf hadoopConf = s3Storage.buildHadoopConfWithReadOnlyConfig(readonlyConfig);
        assertHadoopConfForS3a(hadoopConf);
    }

    private void assertHadoopConfForS3a(HadoopConf s3aConf) {
        Assertions.assertTrue(s3aConf instanceof HiveOnS3Conf);
        Assertions.assertEquals(s3aConf.getSchema(), "s3a");
        Assertions.assertEquals(s3aConf.getFsHdfsImpl(), "org.apache.hadoop.fs.s3a.S3AFileSystem");
    }
}
