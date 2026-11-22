/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.lance.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LanceCatalogLoaderTest {

    private LanceCatalogLoader loaderWith(Map<String, String> storageOptions) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(LanceCommonOptions.KEY_NAMESPACE_TYPE.key(), "dir");
        configs.put(LanceCommonOptions.KEY_ROOT_NAMESPACE_PATH.key(), "s3://bucket/ns");
        if (storageOptions != null) {
            configs.put(LanceSinkOptions.WRITE_STORAGE_OPTIONS.key(), storageOptions);
        }
        return new LanceCatalogLoader(new LanceCommonConfig(ReadonlyConfig.fromMap(configs)));
    }

    @Test
    void namespacePropsTranslateStorageOptionKeysForTheDirectoryNamespace() {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("aws_endpoint_url", "http://127.0.0.1:9000");
        storageOptions.put("aws_access_key_id", "minioadmin");
        storageOptions.put("aws_secret_access_key", "minioadmin");
        storageOptions.put("aws_default_region", "us-east-1");
        storageOptions.put("allow_http", "true");

        Map<String, String> props = loaderWith(storageOptions).buildNamespaceProps();

        // The directory namespace reads "storage."-prefixed properties and builds its OpenDAL
        // operator from endpoint/access_key_id/secret_access_key/region keys.
        Assertions.assertEquals("s3://bucket/ns", props.get("root"));
        Assertions.assertEquals("http://127.0.0.1:9000", props.get("storage.endpoint"));
        Assertions.assertEquals("minioadmin", props.get("storage.access_key_id"));
        Assertions.assertEquals("minioadmin", props.get("storage.secret_access_key"));
        Assertions.assertEquals("us-east-1", props.get("storage.region"));
        Assertions.assertEquals("true", props.get("storage.allow_http"));
    }

    @Test
    void unknownStorageOptionKeysPassThroughUnchanged() {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("custom_key", "custom_value");

        Map<String, String> props = loaderWith(storageOptions).buildNamespaceProps();

        Assertions.assertEquals("custom_value", props.get("storage.custom_key"));
    }

    @Test
    void namespacePropsContainOnlyTheRootWithoutStorageOptions() {
        Map<String, String> props = loaderWith(null).buildNamespaceProps();

        Assertions.assertEquals(1, props.size());
        Assertions.assertEquals("s3://bucket/ns", props.get("root"));
    }
}
