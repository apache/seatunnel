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

package org.apache.seatunnel.resource.yarn.launch;

import org.apache.seatunnel.shade.com.google.common.io.Closer;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "The YARN tests use a Unix container environment")
class YarnLaunchContextTest {
    @TempDir File temporary;

    @Test
    void nativeTarGzRetainsArchiveTypeAndDistributionRoot() throws Exception {
        String distributionRoot = "apache-seatunnel-test-version/";
        File archive = new File(temporary, "native-distribution.tar.gz");
        Closer closer = Closer.create();
        try {
            TarArchiveOutputStream tar =
                    closer.register(
                            new TarArchiveOutputStream(
                                    new GZIPOutputStream(Files.newOutputStream(archive.toPath()))));
            TarArchiveEntry entry =
                    new TarArchiveEntry(distributionRoot + "starter/seatunnel-starter.jar");
            entry.setSize(1);
            tar.putArchiveEntry(entry);
            tar.write(1);
            tar.closeArchiveEntry();
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
        Configuration configuration = new Configuration();
        Path staging = new Path(temporary.toURI());
        try (FileSystem fs = FileSystem.newInstanceLocal(configuration)) {
            FileStatus staged =
                    YarnDistributionSource.register(archive.toURI(), fs, staging, configuration);
            assertEquals("distribution.tar.gz", staged.getPath().getName());
            assertEquals(
                    "seatunnel/" + distributionRoot,
                    YarnDistribution.inspect(fs, staged).localizedHome());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"../starter/seatunnel-starter.jar", "starter\\seatunnel-starter.jar"})
    void unsafeArchiveCannotBeSubmitted(String name) throws Exception {
        File archive = new File(temporary, "unsafe.zip");
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive.toPath()))) {
            zip.putNextEntry(new ZipEntry(name));
            zip.closeEntry();
        }
        try (FileSystem fs = FileSystem.newInstanceLocal(new Configuration())) {
            FileStatus source = fs.getFileStatus(new Path(archive.toURI()));
            assertThrows(
                    IllegalArgumentException.class, () -> YarnDistribution.inspect(fs, source));
        }
    }

    @Test
    void shellArgumentsRemainLiteral() {
        assertEquals(
                "'worker'\"'\"'s $(touch /tmp/unsafe)'",
                YarnContainerCommand.quote("worker's $(touch /tmp/unsafe)"));
    }
}
