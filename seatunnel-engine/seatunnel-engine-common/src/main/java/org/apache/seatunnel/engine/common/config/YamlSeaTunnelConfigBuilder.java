/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.common.config;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;

import com.hazelcast.config.AbstractYamlConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.yaml.YamlDomChecker;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import lombok.NonNull;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.util.Properties;

public class YamlSeaTunnelConfigBuilder extends AbstractYamlConfigBuilder {

    private final InputStream in;

    public YamlSeaTunnelConfigBuilder() {
        this((YamlSeaTunnelConfigLocator) null);
    }

    public YamlSeaTunnelConfigBuilder(YamlSeaTunnelConfigLocator locator) {
        if (locator == null) {
            locator = new YamlSeaTunnelConfigLocator();
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
    }

    public YamlSeaTunnelConfigBuilder(@NonNull InputStream inputStream) {
        this.in = inputStream;
    }

    @Override
    protected String getConfigRoot() {
        return SeaTunnelConfigSections.SEATUNNEL.name;
    }

    public SeaTunnelConfig build() {
        return build(new SeaTunnelConfig());
    }

    public SeaTunnelConfig build(SeaTunnelConfig config) {
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        config.setHazelcastConfig(ConfigProvider.locateAndGetMemberConfig(getProperties()));
        return config;
    }

    private void parseAndBuildConfig(SeaTunnelConfig config) throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = (YamlMapping) YamlLoader.load(in);
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        } finally {
            IOUtil.closeResource(in);
        }

        YamlNode seatunnelRoot = yamlRootNode.childAsMapping(SeaTunnelConfigSections.SEATUNNEL.name);
        if (seatunnelRoot == null) {
            seatunnelRoot = yamlRootNode;
        }

        YamlDomChecker.check(seatunnelRoot);

        Node w3cRootNode = asW3cNode(seatunnelRoot);
        replaceVariables(w3cRootNode);
        importDocuments(seatunnelRoot);

        new YamlSeaTunnelDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    public YamlSeaTunnelConfigBuilder setProperties(Properties properties) {
        if (properties == null) {
            properties = System.getProperties();
        }
        setPropertiesInternal(properties);
        return this;
    }
}
