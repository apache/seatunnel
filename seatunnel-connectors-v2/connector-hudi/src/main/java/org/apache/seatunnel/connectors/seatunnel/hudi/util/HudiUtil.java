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

package org.apache.seatunnel.connectors.seatunnel.hudi.util;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;

public class HudiUtil {

    public static Configuration getConfiguration(String confPaths) {
        Configuration configuration = new Configuration();
        Arrays.stream(confPaths.split(";")).forEach(file -> configuration.addResource(new Path(file)));
        return configuration;
    }

    public static String getParquetFileByPath(String confPaths, String path) throws IOException {
        Configuration configuration = getConfiguration(confPaths);
        FileSystem hdfs = FileSystem.get(configuration);
        Path listFiles = new Path(path);
        FileStatus[] stats = hdfs.listStatus(listFiles);
        for (FileStatus fileStatus : stats) {
            if (fileStatus.isDirectory()) {
                String filePath = getParquetFileByPath(confPaths, fileStatus.getPath().toString());
                if (filePath == null) {
                    continue;
                } else {
                    return filePath;
                }
            }
            if (fileStatus.isFile()) {
                if (fileStatus.getPath().toString().endsWith("parquet")) {
                    return fileStatus.getPath().toString();
                }
            }
        }
        return null;
    }

    public static SeaTunnelRowType getSeaTunnelRowTypeInfo(String confPaths, String path) throws HudiConnectorException {
        Configuration configuration = getConfiguration(confPaths);
        Path dstDir = new Path(path);
        ParquetMetadata footer;
        try {
            footer = ParquetFileReader.readFooter(configuration, dstDir, NO_FILTER);
        } catch (IOException e) {
            throw new HudiConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED,
                "Create ParquetMetadata Fail!", e);
        }
        MessageType schema = footer.getFileMetaData().getSchema();
        String[] fields = new String[schema.getFields().size()];
        SeaTunnelDataType[] types = new SeaTunnelDataType[schema.getFields().size()];

        for (int i = 0; i < schema.getFields().size(); i++) {
            fields[i] = schema.getFields().get(i).getName();
            types[i] = BasicType.STRING_TYPE;
        }
        return new SeaTunnelRowType(fields, types);
    }

    public static JobConf toJobConf(Configuration conf) {
        if (conf instanceof JobConf) {
            return (JobConf) conf;
        }
        return new JobConf(conf);
    }

    public static void initKerberosAuthentication(Configuration conf, String principal, String principalFile) throws HudiConnectorException {
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, principalFile);
        } catch (IOException e) {
            throw new HudiConnectorException(CommonErrorCode.KERBEROS_AUTHORIZED_FAILED,
                "Kerberos Authorized Fail!", e);
        }
    }

}
