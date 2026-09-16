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
package org.apache.seatunnel.connectors.seatunnel.file.adls.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;
import java.util.Map;

public class ADLSHadoopConf extends HadoopConf {
    private String schema = ADLSRuntimeCompatibility.SECURE_ABFS_SCHEME;

    public ADLSHadoopConf(String nameKey) {
        super(nameKey);
    }

    @Override
    public String getFsHdfsImpl() {
        return ADLSRuntimeCompatibility.SECURE_ABFS_IMPLEMENTATION;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public static ADLSHadoopConf buildWithReadOnlyConfig(ReadonlyConfig config) {
        ADLSConfigValidator.validate(config);
        String account = config.get(ADLSFileBaseOptions.ACCOUNT_NAME);
        String container = config.get(ADLSFileBaseOptions.CONTAINER);
        String suffix = config.get(ADLSFileBaseOptions.ENDPOINT_SUFFIX);
        ADLSHadoopConf result =
                new ADLSHadoopConf(
                        ADLSRuntimeCompatibility.secureAbfsUri(account, container, suffix));
        Map<String, String> options = new HashMap<>();
        config.getOptional(ADLSFileBaseOptions.HADOOP_PROPERTIES)
                .ifPresent(values -> values.forEach(options::put));
        ADLSFileBaseOptions.AuthType auth = config.get(ADLSFileBaseOptions.AUTH_TYPE);
        if (auth == ADLSFileBaseOptions.AuthType.SHARED_KEY) {
            options.putAll(
                    ADLSRuntimeCompatibility.sharedKeyOptions(
                            account, suffix, config.get(ADLSFileBaseOptions.ACCOUNT_KEY)));
        } else {
            options.putAll(
                    ADLSRuntimeCompatibility.clientCredentialsOptions(
                            account,
                            suffix,
                            config.get(ADLSFileBaseOptions.AUTHORITY_HOST),
                            config.get(ADLSFileBaseOptions.TENANT_ID),
                            config.get(ADLSFileBaseOptions.CLIENT_ID),
                            config.get(ADLSFileBaseOptions.CLIENT_SECRET)));
        }
        result.setExtraOptions(options);
        return result;
    }
}
