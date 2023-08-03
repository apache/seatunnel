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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Random;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public abstract class ConnectorJar implements IdentifiedDataSerializable {

    protected long connectorJarID;

    protected ConnectorJarType type;

    /** The byte buffer storing the actual data. */
    protected byte[] data;

    protected String pluginName;

    protected String fileName;

    private static final Random RND = new Random();

    public ConnectorJar() {}

    protected ConnectorJar(ConnectorJarType type, String fileName) {
        checkNotNull(type);
        checkNotNull(fileName);
        this.connectorJarID = RND.nextLong();
        this.type = type;
        this.fileName = fileName;
    }

    protected ConnectorJar(ConnectorJarType type, byte[] data, String fileName) {
        //        if (data == null || data.length == 0) {
        //            throw new IllegalArgumentException("The Jar package file for the connector is
        // empty!");
        //        }
        checkNotNull(type);
        checkNotNull(fileName);
        this.connectorJarID = RND.nextLong();
        this.type = type;
        this.data = data;
        this.fileName = fileName;
    }

    protected ConnectorJar(
            long connectorJarID, ConnectorJarType type, byte[] data, String fileName) {
        //        if (data == null || data.length == 0) {
        //            throw new IllegalArgumentException("The Jar package file for the connector is
        // empty!");
        //        }
        checkNotNull(connectorJarID);
        checkNotNull(type);
        checkNotNull(fileName);
        this.connectorJarID = connectorJarID;
        this.type = type;
        this.data = data;
        this.fileName = fileName;
    }

    public static ConnectorJar createConnectorJar(
            ConnectorJarType type, byte[] data, String fileName) {
        if (type == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return new CommonPluginJar(data, fileName);
        } else {
            return new ConnectorPluginJar(data, fileName);
        }
    }

    public static ConnectorJar createConnectorJar(
            long connectorJarID, ConnectorJarType type, byte[] data, String fileName) {
        if (type == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return new CommonPluginJar(connectorJarID, data, fileName);
        } else {
            return new ConnectorPluginJar(connectorJarID, data, fileName);
        }
    }

    public static ConnectorJar createConnectorJar(
            ConnectorJarType type,
            byte[] data,
            PluginIdentifier pluginIdentifier,
            String fileName) {
        if (type == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return new CommonPluginJar(data, pluginIdentifier, fileName);
        } else {
            return new ConnectorPluginJar(data, pluginIdentifier, fileName);
        }
    }

    public static ConnectorJar createConnectorJar(
            long connectorJarID,
            ConnectorJarType type,
            byte[] data,
            PluginIdentifier pluginIdentifier,
            String fileName) {
        if (type == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return new CommonPluginJar(connectorJarID, data, pluginIdentifier, fileName);
        } else {
            return new ConnectorPluginJar(connectorJarID, data, pluginIdentifier, fileName);
        }
    }

    public long getConnectorJarID() {
        return connectorJarID;
    }

    public ConnectorJarType getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

    public String getPluginName() {
        return pluginName;
    }

    public String getFileName() {
        return fileName;
    }
}
