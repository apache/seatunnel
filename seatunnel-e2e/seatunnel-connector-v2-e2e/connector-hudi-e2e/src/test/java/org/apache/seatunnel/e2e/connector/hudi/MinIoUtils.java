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

package org.apache.seatunnel.e2e.connector.hudi;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;

import java.io.FilterInputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class MinIoUtils {

    public static String downloadNewestCommitFile(
            MinioClient minioClient, String bucketName, String pathPrefix, String downloadPath) {
        Iterable<Result<Item>> listObjects =
                minioClient.listObjects(
                        ListObjectsArgs.builder().bucket(bucketName).prefix(pathPrefix).build());
        long newestCommitTime = 0L;
        String objectPath = null;
        for (Result<Item> listObject : listObjects) {
            Item item = null;
            try {
                item = listObject.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (item.isDir() || !item.objectName().endsWith(".parquet")) {
                continue;
            }
            long fileCommitTime =
                    Long.parseLong(
                            item.objectName()
                                    .substring(
                                            item.objectName().lastIndexOf("_") + 1,
                                            item.objectName().lastIndexOf(".parquet")));
            if (fileCommitTime > newestCommitTime) {
                objectPath = item.objectName();
            }
        }
        log.info("download object path: {}", objectPath);
        assert objectPath != null;
        Path path =
                Paths.get(
                        createDir(downloadPath)
                                + objectPath.substring(objectPath.lastIndexOf("/") + 1));
        try (FilterInputStream inputStream =
                        minioClient.getObject(
                                GetObjectArgs.builder()
                                        .bucket(bucketName)
                                        .object(objectPath)
                                        .build());
                OutputStream outputStream = Files.newOutputStream(path)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } catch (Exception e) {
            log.error("download error \n", e);
            throw new RuntimeException(e);
        }
        log.info("download success path: {}", path);
        return path.toFile().getAbsolutePath();
    }

    private static String createDir(String downloadPath) {
        Path path = Paths.get(downloadPath);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return downloadPath;
    }
}
