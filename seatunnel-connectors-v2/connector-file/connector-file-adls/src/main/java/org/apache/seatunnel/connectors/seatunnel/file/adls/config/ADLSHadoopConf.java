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

/**
 * Hadoop filesystem configuration for an ADLS Gen2 container.
 *
 * <p>This class translates the connector's stable options into the account-qualified ABFS keys
 * expected by Hadoop. Keeping that translation in one place prevents source and sink factories from
 * constructing subtly different filesystem or authentication configurations.
 */
public class ADLSHadoopConf extends HadoopConf {
    // ABFSS is the safe default because Hadoop must not send ADLS credentials over plain HTTP.
    private String schema = ADLSRuntimeCompatibility.SECURE_ABFS_SCHEME;
    private String fileSystemImplementation = ADLSRuntimeCompatibility.SECURE_ABFS_IMPLEMENTATION;

    public ADLSHadoopConf(String nameKey) {
        super(nameKey);
    }

    @Override
    public String getFsHdfsImpl() {
        return fileSystemImplementation;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Validates connector options and builds the Hadoop configuration used to create the ADLS
     * filesystem.
     *
     * @param config connector configuration
     * @return account-qualified ADLS Hadoop configuration
     */
    public static ADLSHadoopConf buildWithReadOnlyConfig(ReadonlyConfig config) {
        ADLSConfigValidator.validate(config);
        String account = ADLSConfigValidator.required(config, ADLSFileBaseOptions.ACCOUNT_NAME);
        String container = ADLSConfigValidator.required(config, ADLSFileBaseOptions.CONTAINER);
        String suffix = ADLSConfigValidator.required(config, ADLSFileBaseOptions.ENDPOINT_SUFFIX);
        Map<String, String> options = new HashMap<>();
        // Add the advanced settings first so connector-derived authentication values always win,
        // even if validation is relaxed or bypassed by a future caller.
        config.getOptional(ADLSFileBaseOptions.HADOOP_PROPERTIES)
                .ifPresent(values -> values.forEach(options::put));
        ADLSHadoopConf result =
                new ADLSHadoopConf(
                        ADLSRuntimeCompatibility.secureAbfsUri(account, container, suffix));
        ADLSFileBaseOptions.AuthType auth = config.get(ADLSFileBaseOptions.AUTH_TYPE);
        if (auth == ADLSFileBaseOptions.AuthType.SHARED_KEY) {
            options.putAll(
                    ADLSRuntimeCompatibility.sharedKeyOptions(
                            account,
                            suffix,
                            ADLSConfigValidator.required(config, ADLSFileBaseOptions.ACCOUNT_KEY)));
        } else {
            options.putAll(
                    ADLSRuntimeCompatibility.clientCredentialsOptions(
                            account,
                            suffix,
                            ADLSConfigValidator.required(
                                    config, ADLSFileBaseOptions.AUTHORITY_HOST),
                            ADLSConfigValidator.required(config, ADLSFileBaseOptions.TENANT_ID),
                            ADLSConfigValidator.required(config, ADLSFileBaseOptions.CLIENT_ID),
                            ADLSConfigValidator.required(
                                    config, ADLSFileBaseOptions.CLIENT_SECRET)));
        }
        result.setExtraOptions(options);
        return result;
    }
}
