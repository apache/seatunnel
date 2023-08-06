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

import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.engine.client.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.protocol.codec.SeaTunnelUploadConnectorJarCodec;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class ConnectorPackageClient {

    private static final ILogger LOGGER = Logger.getLogger(ConnectorPackageClient.class);

    private final SeaTunnelHazelcastClient hazelcastClient;

    public ConnectorPackageClient(SeaTunnelHazelcastClient hazelcastClient) {
        checkNotNull(hazelcastClient);
        this.hazelcastClient = hazelcastClient;
    }

    public Set<URL> uploadCommonPluginJars(long jobId, List<URL> commonPluginJars)
            throws MalformedURLException {
        Set<URL> pluginJarsStoragePathSet = new HashSet<>();
        // Upload commonPluginJar
        for (URL commonPluginJar : commonPluginJars) {
            Path path = Paths.get(commonPluginJar.getPath().substring(1));
            // Obtain the directory name of the relative location of the file path.
            int directoryIndex = path.getNameCount() - 3;
            String pluginName = path.getName(directoryIndex).toString();
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(null, null, pluginName);
            Optional<URL> optional = uploadCommonPluginJar(jobId, path, pluginIdentifier);
            if (optional.isPresent()) {
                pluginJarsStoragePathSet.add(optional.get());
            }
        }
        return pluginJarsStoragePathSet;
    }

    private Optional<URL> uploadCommonPluginJar(
            long jobId, Path commonPluginJar, PluginIdentifier pluginIdentifier) {
        byte[] data = new byte[0];
        try {
            data = readFileData(commonPluginJar);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String fileName = commonPluginJar.getFileName().toString();
        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        ConnectorJarType.COMMON_PLUGIN_JAR, data, pluginIdentifier, fileName);
        String connectorJarStoragePath =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelUploadConnectorJarCodec.encodeRequest(
                                jobId,
                                hazelcastClient.getSerializationService().toData(connectorJar)),
                        SeaTunnelUploadConnectorJarCodec::decodeResponse);
        File storageFile = new File(connectorJarStoragePath);
        try {
            return Optional.of(storageFile.toURI().toURL());
        } catch (MalformedURLException e) {
            LOGGER.warning(String.format("Cannot get plugin URL: {%s}", storageFile));
            return Optional.empty();
        }
    }

    public Optional<URL> uploadConnectorPluginJar(long jobId, URL connectorPluginJarURL) {
        Path connectorPluginJarPath = Paths.get(connectorPluginJarURL.getPath().substring(1));
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(CollectionConstants.SEATUNNEL_PLUGIN, null, null);

        byte[] data = new byte[0];
        try {
            data = readFileData(connectorPluginJarPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String fileName = connectorPluginJarPath.getFileName().toString();
        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, data, pluginIdentifier, fileName);
        String connectorJarStoragePath =
                hazelcastClient.requestOnMasterAndDecodeResponse(
                        SeaTunnelUploadConnectorJarCodec.encodeRequest(
                                jobId,
                                hazelcastClient.getSerializationService().toData(connectorJar)),
                        SeaTunnelUploadConnectorJarCodec::decodeResponse);

        File storageFile = new File(connectorJarStoragePath);
        try {
            return Optional.of(storageFile.toURI().toURL());
        } catch (MalformedURLException e) {
            LOGGER.warning(String.format("Cannot get plugin URL: {%s}", storageFile));
            return Optional.empty();
        }
    }

    private static byte[] readFileData(Path filePath) throws IOException {
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
            return new byte[0];
        }
    }
}
