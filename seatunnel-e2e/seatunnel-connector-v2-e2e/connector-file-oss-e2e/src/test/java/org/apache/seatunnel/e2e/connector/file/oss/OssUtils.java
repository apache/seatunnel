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

package org.apache.seatunnel.e2e.connector.file.oss;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.PutObjectResult;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class OssUtils {
    private static Logger logger = LoggerFactory.getLogger(OssUtils.class);
    private OSS ossClient = null;
    private String endpoint = "https://oss-accelerate.aliyuncs.com";
    private String accessKeyId = "xxxxxxxxxxxxxxxxxxx";
    private String accessKeySecret = "xxxxxxxxxxxxxxxxxxx";
    private String bucket = "whale-ops";

    public OssUtils() {
        OSSClientBuilder ossClientBuilder = new OSSClientBuilder();
        ossClient = ossClientBuilder.build(endpoint, accessKeyId, accessKeySecret);
    }

    public void uploadTestFiles(
            String filePath, String targetFilePath, boolean isFindFromResource) {
        try {
            File resourcesFile = null;
            if (isFindFromResource) {
                resourcesFile = ContainerUtil.getResourcesFile(filePath);
            } else {
                resourcesFile = new File(filePath);
            }
            FileInputStream fileInputStream = new FileInputStream(resourcesFile);
            PutObjectResult result = ossClient.putObject(bucket, targetFilePath, fileInputStream);
        } catch (OSSException oe) {
            logger.error(
                    "Caught an OSSException, which means your request made it to OSS, "
                            + "but was rejected with an error response for some reason.");
            logger.error("Error Message:" + oe.getErrorMessage());
            logger.error("Error Code:" + oe.getErrorCode());
            logger.error("Request ID:" + oe.getRequestId());
            logger.error("Host ID:" + oe.getHostId());
        } catch (ClientException ce) {
            logger.error(
                    "Caught an ClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with OSS, "
                            + "such as not being able to access the network.");
            logger.error("Error Message:" + ce.getMessage());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void createDir(String dir) {
        try {
            PutObjectResult result =
                    ossClient.putObject(bucket, dir, new ByteArrayInputStream("".getBytes()));
        } catch (OSSException oe) {
            logger.error(
                    "Caught an OSSException, which means your request made it to OSS, "
                            + "but was rejected with an error response for some reason.");
            logger.error("Error Message:" + oe.getErrorMessage());
            logger.error("Error Code:" + oe.getErrorCode());
            logger.error("Request ID:" + oe.getRequestId());
            logger.error("Host ID:" + oe.getHostId());
        } catch (ClientException ce) {
            logger.error(
                    "Caught an ClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with OSS, "
                            + "such as not being able to access the network.");
            logger.error("Error Message:" + ce.getMessage());
        }
    }

    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }
}
