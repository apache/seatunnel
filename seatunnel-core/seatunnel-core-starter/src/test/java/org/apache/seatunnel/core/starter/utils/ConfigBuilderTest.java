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

package org.apache.seatunnel.core.starter.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConfigBuilderTest {

    @Test
    public void testConfigDesensitizationSort() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("a", "1");
        config.put("b", "1");
        config.put("c", "1");
        config.put("d", "1");
        config.put("e", "1");
        config.put("f", "1");

        Map<String, Object> desensitizationConfig =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getSensitiveOptions(null));
        List<String> keys = new ArrayList<>(desensitizationConfig.keySet());
        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c", "d", "e", "f"), keys);
    }

    @Test
    public void testConfigDesensitizationMasksJdbcUrls() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put(
                "url",
                "jdbc:mysql://alice:secret-password@db.example.com:3306/orders?token=secret-token");
        source.put("metadata_url", "https://catalog.example.com/tables");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(source));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getSensitiveOptions(null));
        List<?> sources = (List<?>) desensitized.get("source");
        Map<?, ?> desensitizedSource = (Map<?, ?>) sources.get(0);

        Assertions.assertEquals("******", desensitizedSource.get("url"));
        Assertions.assertEquals(
                "https://catalog.example.com/tables", desensitizedSource.get("metadata_url"));
    }

    @Test
    public void testConfigDesensitizationMasksKafkaJaasConfig() {
        Map<String, Object> jaasConfig = new LinkedHashMap<>();
        jaasConfig.put(
                "config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"alice\" password=\"secret\";");

        Map<String, Object> saslConfig = new LinkedHashMap<>();
        saslConfig.put("jaas", jaasConfig);

        Map<String, Object> kafkaConfig = new LinkedHashMap<>();
        kafkaConfig.put("bootstrap.servers", "localhost:9092");
        kafkaConfig.put("sasl", saslConfig);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("kafka.config", kafkaConfig);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("source", Arrays.asList(source));

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getSensitiveOptions(null));
        List<?> sources = (List<?>) desensitized.get("source");
        Map<?, ?> desensitizedSource = (Map<?, ?>) sources.get(0);
        Map<?, ?> desensitizedKafkaConfig = (Map<?, ?>) desensitizedSource.get("kafka.config");
        Map<?, ?> desensitizedSaslConfig = (Map<?, ?>) desensitizedKafkaConfig.get("sasl");
        Map<?, ?> desensitizedJaasConfig = (Map<?, ?>) desensitizedSaslConfig.get("jaas");

        Assertions.assertEquals("******", desensitizedJaasConfig.get("config"));
        Assertions.assertEquals("localhost:9092", desensitizedKafkaConfig.get("bootstrap.servers"));
    }

    @Test
    public void testConfigDesensitizationMasksS3CredentialOptions() {
        Map<String, Object> accessKeyConfig = new LinkedHashMap<>();
        accessKeyConfig.put("key", "access-key");

        Map<String, Object> accessConfig = new LinkedHashMap<>();
        accessConfig.put("access", accessKeyConfig);

        Map<String, Object> s3aConfig = new LinkedHashMap<>();
        s3aConfig.put("endpoint", "http://localhost:9000");
        s3aConfig.putAll(accessConfig);

        Map<String, Object> fsConfig = new LinkedHashMap<>();
        fsConfig.put("s3a", s3aConfig);

        Map<String, Object> checkpointConfig = new LinkedHashMap<>();
        checkpointConfig.put("fs", fsConfig);
        checkpointConfig.put("fs.s3a.secret.key", "secret-key");

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("checkpoint", checkpointConfig);

        Map<String, Object> desensitized =
                ConfigBuilder.configDesensitization(
                        config, ConfigShadeUtils.getSensitiveOptions(null));
        Map<?, ?> desensitizedCheckpoint = (Map<?, ?>) desensitized.get("checkpoint");
        Map<?, ?> desensitizedFsConfig = (Map<?, ?>) desensitizedCheckpoint.get("fs");
        Map<?, ?> desensitizedS3aConfig = (Map<?, ?>) desensitizedFsConfig.get("s3a");
        Map<?, ?> desensitizedAccessConfig = (Map<?, ?>) desensitizedS3aConfig.get("access");

        Assertions.assertEquals("******", desensitizedAccessConfig.get("key"));
        Assertions.assertEquals("******", desensitizedCheckpoint.get("fs.s3a.secret.key"));
        Assertions.assertEquals("http://localhost:9000", desensitizedS3aConfig.get("endpoint"));
    }
}
