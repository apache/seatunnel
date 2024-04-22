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

package org.apache.seatunnel.connectors.seatunnel.hive.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Conf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3ConfigOptions;

public class HiveOnS3Conf extends S3Conf {
    protected static final String S3_SCHEMA = "s3";
    // The emr of amazon on s3 use this EmrFileSystem as the file system
    protected static final String HDFS_S3_IMPL = "com.amazon.ws.emr.hadoop.fs.EmrFileSystem";

    protected HiveOnS3Conf(String hdfsNameKey, String schema) {
        super(hdfsNameKey);
        setSchema(schema);
    }

    @Override
    public String getFsHdfsImpl() {
        return switchHdfsImpl();
    }

    @Override
    protected String switchHdfsImpl() {
        return getSchema().equals(S3_SCHEMA) ? HDFS_S3_IMPL : super.switchHdfsImpl();
    }

    public static HadoopConf buildWithReadOnlyConfig(ReadonlyConfig readonlyConfig) {
        S3Conf s3Conf = (S3Conf) S3Conf.buildWithReadOnlyConfig(readonlyConfig);
        String bucketName = readonlyConfig.get(S3ConfigOptions.S3_BUCKET);
        if (bucketName.startsWith(DEFAULT_SCHEMA)) {
            s3Conf.setSchema(DEFAULT_SCHEMA);
        } else if (bucketName.startsWith(S3A_SCHEMA)) {
            s3Conf.setSchema(S3A_SCHEMA);
        } else {
            s3Conf.setSchema(S3_SCHEMA);
        }
        return new HiveOnS3Conf(s3Conf.getHdfsNameKey(), s3Conf.getSchema());
    }
}
