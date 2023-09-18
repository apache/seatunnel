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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.utils.MDUtil;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Files;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConnectorPackageHAStorage extends AbstractConnectorPackageHAStorage {

    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public ConnectorPackageHAStorage(ConnectorJarStorageConfig connectorJarStorageConfig) {
        super(connectorJarStorageConfig);
    }

    @Override
    public void uploadConnectorJar(
            long jobId, File localFile, ConnectorJarIdentifier connectorJarIdentifier) {
        String storageLocationPath = getStorageLocationPath(jobId, connectorJarIdentifier);
        try {
            if (!fileSystem.exists(new Path(storageLocationPath))) {
                FSDataOutputStream os = fileSystem.create(new Path(storageLocationPath));
                LOGGER.info(String.format("Copying from %s to {}.", localFile, storageLocationPath));
                Files.copy(localFile, os);
                os.hsync();
            }
        } catch (IOException e) {
            LOGGER.info(
                    String.format(
                            "Upload connector jar package from %s to %s has Failed!",
                            localFile, storageLocationPath));
        }
    }

    @Override
    public void downloadConnectorJar(long jobId, ConnectorJarIdentifier connectorJarIdentifier) {
        final int buffSize = 4096; // BLOCKSIZE, for chunked file copying
        Path fromPath = new Path(getStorageLocationPath(jobId, connectorJarIdentifier));
        File toFile = new File(connectorJarIdentifier.getStoragePath());
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        // compute the digest of the file
        MessageDigest messageDigest = MDUtil.createMessageDigest();
        try {
            InputStream is = fileSystem.open(fromPath);
            LOGGER.warning(
                    String.format("Copying from %s to %s.", fromPath, byteArrayOutputStream));
            // create a hash on-the-fly
            final byte[] buf = new byte[buffSize];
            int bytesRead = is.read(buf);
            while (bytesRead >= 0) {
                byteArrayOutputStream.write(buf, 0, bytesRead);
                messageDigest.update(buf, 0, bytesRead);
                bytesRead = is.read(buf);
            }
            // verify that file contents are correct
            final byte[] computedKey = messageDigest.digest();
            if (!Arrays.equals(computedKey, connectorJarIdentifier.getConnectorJarID())) {
                throw new IOException("Detected data corruption during transfer");
            }
            readWriteLock.writeLock().lock();
            FileUtils.writeByteArrayToFile(toFile, byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            LOGGER.warning(
                    String.format(
                            "Download connector jar: %s from HA Storage: %s failed!",
                            connectorJarIdentifier.getFileName(), fromPath));
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteConnectorJar(long jobId, ConnectorJarIdentifier connectorJarIdentifier) {
        String storageLocationPath = getStorageLocationPath(jobId, connectorJarIdentifier);
        deleteConnectorJar(storageLocationPath);
    }

    private boolean deleteConnectorJar(String storageLocationPath) {
        try {
            LOGGER.warning(String.format("Deleting %s.", storageLocationPath));

            Path path = new Path(storageLocationPath);

            boolean result = true;
            readWriteLock.writeLock().lock();
            if (fileSystem.exists(path)) {
                result = fileSystem.delete(path, true);
            } else {
                LOGGER.warning(
                        String.format(
                                "The given path %s is not present anymore. No deletion is required.",
                                storageLocationPath));
            }
            // send a call to delete the directory containing the file. This will
            // fail (and be ignored) when some files still exist.
            try {
                fileSystem.delete(path.getParent(), false);
                fileSystem.delete(new Path(basePath), false);
            } catch (IOException ignored) {
            }
            return result;
        } catch (Exception e) {
            LOGGER.warning(String.format("Failed to delete blob at %s.", storageLocationPath));
            return false;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
