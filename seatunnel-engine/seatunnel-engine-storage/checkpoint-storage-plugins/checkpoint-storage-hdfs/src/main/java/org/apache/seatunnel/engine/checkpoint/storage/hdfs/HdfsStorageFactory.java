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

import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.HdfsFileStorageInstance;

import com.google.auto.service.AutoService;

import java.util.Map;

/**
 * HdfsCheckpointStorageFactory. if you want to use HdfsCheckpointStorage, you should add the
 * following configuration in the configuration file:
 *
 * <pre>
 *      storage.type = hdfs # hdfs, local(default),s3, oss
 *  </pre>
 *
 * then you need to configure the following parameters by the storage.type: hdfs {@link
 * org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.HdfsConfiguration} local {@link
 * org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.LocalConfiguration} s3 {@link
 * org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.S3Configuration} eg: s3
 *
 * <pre>
 *      storage.type = "s3"
 *      s3.assess.key = "your access key"
 *      s3.script.key = "your script key"
 *      s3.bucket= "s3a://your bucket"
 *      fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
 *  </pre>
 *
 * oss {@link org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.OssConfiguration} eg: oss
 *
 * <pre>
 *      storage.type = "oss"
 *      fs.oss.accessKeyId = "your access key"
 *      fs.oss.accessKeySecret = "your script key"
 *      fs.oss.endpoint = "such as: oss-cn-hangzhou.aliyuncs.com"
 *      oss.bucket= "oss://your bucket"
 *  </pre>
 */
@AutoService(CheckpointStorageFactory.class)
public class HdfsStorageFactory implements CheckpointStorageFactory {
    @Override
    public String factoryIdentifier() {
        return "hdfs";
    }

    @Override
    public CheckpointStorage create(Map<String, String> configuration)
            throws CheckpointStorageException {
        if (HdfsFileStorageInstance.isFsNull()) {
            return HdfsFileStorageInstance.getOrCreateStorage(configuration);
        }
        return HdfsFileStorageInstance.getHdfsStorage();
    }
}
