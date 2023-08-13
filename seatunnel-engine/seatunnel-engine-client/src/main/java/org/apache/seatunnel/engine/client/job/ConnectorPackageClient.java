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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.common.utils.MDUtil;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelUploadConnectorJarCodec;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class ConnectorPackageClient {

    private static final ILogger LOGGER = Logger.getLogger(ConnectorPackageClient.class);

    private final SeaTunnelHazelcastClient hazelcastClient;

    public ConnectorPackageClient(SeaTunnelHazelcastClient hazelcastClient) {
        checkNotNull(hazelcastClient);
        this.hazelcastClient = hazelcastClient;
    }

    public Set<ConnectorJarIdentifier> uploadCommonPluginJars(long jobId, List<URL> commonPluginJars) {
        Set<ConnectorJarIdentifier> connectorJarIdentifiers = new HashSet<>();
        // Upload commonPluginJar
        for (URL commonPluginJar : commonPluginJars) {
            Path path = Paths.get(commonPluginJar.getPath().substring(1));
            // Obtain the directory name of the relative location of the file path.
            int directoryIndex = path.getNameCount() - 3;
            String pluginName = path.getName(directoryIndex).toString();
            ConnectorJarIdentifier connectorJarIdentifier = uploadCommonPluginJar(jobId, path, pluginName);
            connectorJarIdentifiers.add(connectorJarIdentifier);
        }
        return connectorJarIdentifiers;
    }

    private ConnectorJarIdentifier uploadCommonPluginJar(
            long jobId, Path commonPluginJar, String pluginName) {
        byte[] data = readFileData(commonPluginJar);
        String fileName = commonPluginJar.getFileName().toString();

        // compute the digest of the file
        MessageDigest messageDigest = MDUtil.createMessageDigest();
        byte[] digest = messageDigest.digest(data);

        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        digest, ConnectorJarType.COMMON_PLUGIN_JAR, data, pluginName, fileName);
        ConnectorJarIdentifier connectorJarIdentifier = hazelcastClient
                .getSerializationService()
                .toObject(hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelUploadConnectorJarCodec.encodeRequest(
                                jobId,
                                hazelcastClient.getSerializationService().toData(connectorJar)),
                        SeaTunnelUploadConnectorJarCodec::decodeResponse));
        return connectorJarIdentifier;
    }

    public ConnectorJarIdentifier uploadConnectorPluginJar(long jobId, URL connectorPluginJarURL) {
        Path connectorPluginJarPath = Paths.get(connectorPluginJarURL.getPath().substring(1));

        byte[] data = readFileData(connectorPluginJarPath);
        String fileName = connectorPluginJarPath.getFileName().toString();

        // compute the digest of the file
        MessageDigest messageDigest = MDUtil.createMessageDigest();
        byte[] digest = messageDigest.digest(data);

        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        digest, ConnectorJarType.CONNECTOR_PLUGIN_JAR, data, fileName);
        ConnectorJarIdentifier connectorJarIdentifier = hazelcastClient
                .getSerializationService()
                .toObject(hazelcastClient.requestOnMasterAndDecodeResponse(
                SeaTunnelUploadConnectorJarCodec.encodeRequest(
                        jobId,
                        hazelcastClient.getSerializationService().toData(connectorJar)),
                SeaTunnelUploadConnectorJarCodec::decodeResponse));
        return connectorJarIdentifier;
    }

    private static byte[] readFileData(Path filePath) {
        // Read file data and convert it to a byte array.
        try {
            InputStream inputStream = Files.newInputStream(filePath);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.warning(
                    String.format(
                            "Failed to read the connector jar package file : { %s } , the file to be read may not exist",
                            filePath.toString()));
            throw new RuntimeException();
        }
    }
}
