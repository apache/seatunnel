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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsHadoopConfTest {

    @Test
    void shouldConfigureGsFileSystemAndUseDefaultCredentialsWhenKeyFileIsAbsent() throws Exception {
        HadoopConf hadoopConf = build(Collections.emptyMap());

        assertEquals("gs", hadoopConf.getSchema());
        assertEquals("gs://test-bucket", hadoopConf.getHdfsNameKey());
        assertEquals(GcsHadoopConf.GCS_FILESYSTEM_IMPLEMENTATION, hadoopConf.getFsHdfsImpl());
        assertFalse(
                hadoopConf
                        .getExtraOptions()
                        .containsKey(GcsHadoopConf.GCS_SERVICE_ACCOUNT_KEY_FILE));

        Configuration configuration = hadoopConf.toConfiguration();
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        assertEquals("gs://test-bucket", configuration.get("fs.defaultFS"));
        assertEquals(GcsHadoopConf.GCS_FILESYSTEM_IMPLEMENTATION, configuration.get("fs.gs.impl"));
        assertTrue(configuration.getBoolean("fs.gs.impl.disable.cache", false));
        assertEquals(
                GcsHadoopConf.GCS_FILESYSTEM_IMPLEMENTATION,
                FileSystem.getFileSystemClass("gs", configuration).getName());
    }

    @Test
    void shouldConfigureServiceAccountJsonKeyFile() {
        Map<String, Object> options = new HashMap<>();
        options.put(GcsFileBaseOptions.SERVICE_ACCOUNT_KEY_FILE.key(), "/keys/gcs.json");

        HadoopConf hadoopConf = build(options);

        assertEquals(
                "/keys/gcs.json",
                hadoopConf.getExtraOptions().get(GcsHadoopConf.GCS_SERVICE_ACCOUNT_KEY_FILE));
    }

    @Test
    void shouldApplyAdditionalPropertiesAndPreferDedicatedKeyFileOption() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.gs.project.id", "test-project");
        properties.put(GcsHadoopConf.GCS_SERVICE_ACCOUNT_KEY_FILE, "/keys/from-map.json");

        Map<String, Object> options = new HashMap<>();
        options.put(GcsFileBaseOptions.GCS_PROPERTIES.key(), properties);
        options.put(GcsFileBaseOptions.SERVICE_ACCOUNT_KEY_FILE.key(), "/keys/explicit.json");

        HadoopConf hadoopConf = build(options);

        assertEquals("test-project", hadoopConf.getExtraOptions().get("fs.gs.project.id"));
        assertEquals(
                "/keys/explicit.json",
                hadoopConf.getExtraOptions().get(GcsHadoopConf.GCS_SERVICE_ACCOUNT_KEY_FILE));
    }

    @Test
    void shouldRejectBlankServiceAccountKeyFile() {
        Map<String, Object> options = new HashMap<>();
        options.put(GcsFileBaseOptions.SERVICE_ACCOUNT_KEY_FILE.key(), "  ");

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> build(options));

        assertTrue(exception.getMessage().contains("must not be blank"));
    }

    @Test
    void shouldRejectInvalidBucketUris() {
        String[] invalidBuckets = {
            "test-bucket",
            "s3a://test-bucket",
            "gs://test-bucket/path",
            "gs://user@test-bucket",
            "gs://test-bucket?query=value",
            "gs://test-bucket#fragment",
            "gs://"
        };

        for (String invalidBucket : invalidBuckets) {
            Map<String, Object> options = new HashMap<>();
            options.put(GcsFileBaseOptions.BUCKET.key(), invalidBucket);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> GcsHadoopConf.buildWithReadonlyConfig(ReadonlyConfig.fromMap(options)),
                    invalidBucket);
        }
    }

    private static HadoopConf build(Map<String, Object> options) {
        Map<String, Object> config = new HashMap<>(options);
        config.put(GcsFileBaseOptions.BUCKET.key(), "gs://test-bucket");
        return GcsHadoopConf.buildWithReadonlyConfig(ReadonlyConfig.fromMap(config));
    }
}
