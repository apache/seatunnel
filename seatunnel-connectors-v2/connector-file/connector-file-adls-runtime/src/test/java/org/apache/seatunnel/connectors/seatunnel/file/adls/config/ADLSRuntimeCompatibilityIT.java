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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class ADLSRuntimeCompatibilityIT {
    @Test
    void performsAbfsFileLifecycleAgainstConfiguredStorage() throws Exception {
        String account = System.getenv("SEATUNNEL_ADLS_ACCOUNT");
        String container = System.getenv("SEATUNNEL_ADLS_CONTAINER");
        String key = System.getenv("SEATUNNEL_ADLS_ACCOUNT_KEY");
        String prefix = System.getenv("SEATUNNEL_ADLS_TEST_PREFIX");
        Assumptions.assumeTrue(
                "true".equalsIgnoreCase(System.getenv("SEATUNNEL_ADLS_IT")),
                "Set SEATUNNEL_ADLS_IT=true to run the Azure integration test");
        Assumptions.assumeTrue(
                account != null && container != null && key != null && prefix != null,
                "ADLS account, container, account key and test prefix are required");

        Configuration configuration = ADLSRuntimeCompatibility.newConfiguration(account, container);
        ADLSRuntimeCompatibility.configureSharedKey(configuration, account, key);
        Path path = new Path(prefix + "/phase0-runtime.txt");
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            fileSystem.mkdirs(path.getParent());
            try (FSDataOutputStream output = fileSystem.create(path, true)) {
                output.write("phase0".getBytes(StandardCharsets.UTF_8));
            }
            try (FSDataInputStream input = fileSystem.open(path)) {
                byte[] value = new byte[6];
                input.readFully(value);
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                        "phase0".getBytes(StandardCharsets.UTF_8), value);
            }
            org.junit.jupiter.api.Assertions.assertTrue(
                    fileSystem.rename(path, new Path(prefix + "/phase0-runtime-renamed.txt")));
            fileSystem.delete(new Path(prefix), true);
        }
    }
}
