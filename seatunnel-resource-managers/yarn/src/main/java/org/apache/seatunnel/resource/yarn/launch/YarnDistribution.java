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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** Records the distribution's native archive layout instead of requiring users to repack it. */
public final class YarnDistribution {
    /** Private manifest describing the archive name and its discovered top-level directory. */
    private static final String MANIFEST = "distribution.properties";

    /** Manifest property containing the localized archive file name. */
    private static final String ARCHIVE_PROPERTY = "archive";

    /** Manifest property containing the path prefix before the SeaTunnel distribution. */
    private static final String ROOT_PROPERTY = "root";

    /** Required file used to identify exactly one valid SeaTunnel distribution root. */
    private static final String STARTER_MARKER = "starter/seatunnel-starter.jar";

    /** Stable localized names selected from the source archive format. */
    private static final String LOCALIZED_ZIP = "distribution.zip";

    private static final String LOCALIZED_TAR_GZ = "distribution.tar.gz";
    private final String archiveName;
    private final String root;

    private YarnDistribution(String archiveName, String root) {
        this.archiveName = archiveName;
        this.root = root;
    }

    /**
     * Validates an archive and discovers the distribution root before allocating an application.
     */
    public static YarnDistribution inspect(File archive) throws IOException {
        String name = archive.getName();
        String distributionRoot = null;
        if (name.endsWith(".zip")) {
            try (ZipFile zip = new ZipFile(archive)) {
                Enumeration<? extends ZipEntry> entries = zip.entries();
                while (entries.hasMoreElements()) {
                    distributionRoot =
                            detectRoot(entries.nextElement().getName(), distributionRoot);
                }
            }
            return new YarnDistribution(LOCALIZED_ZIP, requireRoot(distributionRoot));
        }
        if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
            try (TarArchiveInputStream tar =
                    new TarArchiveInputStream(
                            new GZIPInputStream(Files.newInputStream(archive.toPath())))) {
                TarArchiveEntry entry;
                while ((entry = tar.getNextTarEntry()) != null) {
                    distributionRoot = detectRoot(entry.getName(), distributionRoot);
                }
            }
            return new YarnDistribution(LOCALIZED_TAR_GZ, requireRoot(distributionRoot));
        }
        throw new IllegalArgumentException(
                "yarn.distribution must be a .tar.gz, .tgz or .zip archive");
    }

    private static String requireRoot(String root) {
        if (root == null) {
            throw new IllegalArgumentException(
                    "Distribution archive must contain starter/seatunnel-starter.jar (optionally inside one top-level directory)");
        }
        return root;
    }

    private static String detectRoot(String name, String previous) {
        while (name.startsWith("./")) {
            name = name.substring(2);
        }
        if (name.startsWith("/")
                || name.contains("\\")
                || Arrays.asList(name.split("/")).contains("..")) {
            throw new IllegalArgumentException("Unsafe distribution archive entry: " + name);
        }
        if (name.equals(STARTER_MARKER) || name.endsWith("/" + STARTER_MARKER)) {
            String root = name.substring(0, name.length() - STARTER_MARKER.length());
            if (previous != null && !previous.equals(root)) {
                throw new IllegalArgumentException(
                        "Distribution contains multiple SeaTunnel installations");
            }
            return root;
        }
        return previous;
    }

    /** Uploads the archive and its layout manifest to the private application directory. */
    public void stage(FileSystem fileSystem, Path staging, File source) throws IOException {
        fileSystem.copyFromLocalFile(new Path(source.toURI()), new Path(staging, archiveName));
        Properties manifest = new Properties();
        manifest.setProperty(ARCHIVE_PROPERTY, archiveName);
        manifest.setProperty(ROOT_PROPERTY, root);
        try (OutputStream output = fileSystem.create(new Path(staging, MANIFEST), false)) {
            manifest.store(output, "SeaTunnel localized distribution");
        }
    }

    static YarnDistribution read(FileSystem fileSystem, Path staging) throws IOException {
        Properties manifest = new Properties();
        try (InputStream input = fileSystem.open(new Path(staging, MANIFEST))) {
            manifest.load(input);
        }
        return new YarnDistribution(
                manifest.getProperty(ARCHIVE_PROPERTY), manifest.getProperty(ROOT_PROPERTY));
    }

    Path archive(Path staging) {
        return new Path(staging, archiveName);
    }

    String localizedHome() {
        return YarnConstants.LOCALIZED_DISTRIBUTION_NAME + "/" + root;
    }
}
