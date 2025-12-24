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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

import java.util.HashMap;
import java.util.Map;

@Disabled(
        "OSS is not available in CI, if you want to run this test, please set up your own oss environment")
public class OssFileCheckpointTest extends AbstractFileCheckPointTest {
    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        Map<String, String> config = new HashMap<>();
        config.put("storage.type", "oss");
        config.put("disable.cache", "false");
        config.put("fs.oss.accessKeyId", "your access key id");
        config.put("fs.oss.accessKeySecret", "your access key secret");
        config.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        config.put("oss.bucket", "oss://seatunnel-test/");
        STORAGE = new HdfsStorage(config);
        initStorageData();
    }
}
