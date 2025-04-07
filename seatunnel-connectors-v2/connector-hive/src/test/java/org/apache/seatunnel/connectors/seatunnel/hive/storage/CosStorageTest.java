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
import org.apache.seatunnel.connectors.seatunnel.file.cos.config.CosConf;
import org.apache.seatunnel.connectors.seatunnel.file.cos.config.CosConfigOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;

public class CosStorageTest {

    private static final ReadonlyConfig COS =
            ReadonlyConfig.fromMap(
                    new HashMap<String, Object>() {
                        {
                            put(
                                    "hive.hadoop.conf",
                                    new HashMap<String, String>() {
                                        {
                                            put("bucket", "cosn://my_bucket");
                                            put(CosConfigOptions.SECRET_ID.key(), "test");
                                            put(CosConfigOptions.SECRET_KEY.key(), "test");
                                            put(CosConfigOptions.REGION.key(), "ap-shanghai");
                                        }
                                    });
                        }
                    });

    @Test
    void fillBucketInHadoopConf() {
        COSStorage cosStorage = new COSStorage();
        HadoopConf cosnConf = cosStorage.buildHadoopConfWithReadOnlyConfig(COS);
        assertHadoopConf(cosnConf);
    }

    @Test
    void fillBucketInHadoopConfPath() throws URISyntaxException {
        URL resource = CosStorageTest.class.getResource("/cos");
        String filePath = Paths.get(resource.toURI()).toString();
        HashMap<String, Object> map = new HashMap<>();
        map.put("hive.hadoop.conf-path", filePath);
        map.putAll(COS.toMap());
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(map);
        COSStorage cosStorage = new COSStorage();
        HadoopConf hadoopConf = cosStorage.buildHadoopConfWithReadOnlyConfig(readonlyConfig);
        assertHadoopConf(hadoopConf);
    }

    private static void assertHadoopConf(HadoopConf cosnConf) {
        Assertions.assertTrue(cosnConf instanceof CosConf);
        Assertions.assertEquals(cosnConf.getSchema(), "cosn");
        Assertions.assertEquals(cosnConf.getFsHdfsImpl(), "org.apache.hadoop.fs.CosFileSystem");
    }
}
