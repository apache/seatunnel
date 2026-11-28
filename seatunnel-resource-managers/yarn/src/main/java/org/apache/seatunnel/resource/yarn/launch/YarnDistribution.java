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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/** An inspected archive, its localization metadata and the SeaTunnel installation inside it. */
public final class YarnDistribution {
    /** Qualified path of the archive registered for localization. */
    private static final String ARCHIVE_PROPERTY = "archive";

    private static final String SIZE_PROPERTY = "archive.size";
    private static final String TIMESTAMP_PROPERTY = "archive.timestamp";

    /** Manifest property containing the path prefix before the SeaTunnel distribution. */
    private static final String ROOT_PROPERTY = "root";

    /** Required file used to identify exactly one valid SeaTunnel distribution root. */
    private static final String STARTER_MARKER = "starter/seatunnel-starter.jar";

    /** Stable localized names selected from the source archive format. */
    private static final String LOCALIZED_ZIP = "distribution.zip";

    private static final String LOCALIZED_TAR_GZ = "distribution.tar.gz";
    private final Set<String> configFiles;
    @Getter private final Path path;
    @Getter private final long size;
    @Getter private final long timestamp;
    private final String root;

    private YarnDistribution(FileStatus archive, String root, Set<String> configFiles) {
        this(archive.getPath(), archive.getLen(), archive.getModificationTime(), root, configFiles);
    }

    private YarnDistribution(
            Path path, long size, long timestamp, String root, Set<String> configFiles) {
        this.path = path;
        this.size = size;
        this.timestamp = timestamp;
        this.configFiles = configFiles;
        this.root = root;
    }

    /** Reads the layout from the same file that the NodeManager will later unpack. */
    static YarnDistribution inspect(FileSystem fileSystem, FileStatus archive) throws IOException {
        String name = archiveName(archive.getPath().getName());
        String distributionRoot = null;
        Set<String> configFiles = new HashSet<>();
        Closer closer = Closer.create();
        try {
            if (LOCALIZED_ZIP.equals(name)) {
                HadoopArchiveChannel channel =
                        closer.register(
                                new HadoopArchiveChannel(
                                        fileSystem.open(archive.getPath()), archive.getLen()));
                // The layout only needs the central directory. Seeking to every local header would
                // add a remote read per entry on HDFS.
                ZipFile zip =
                        closer.register(
                                new ZipFile(
                                        channel,
                                        archive.getPath().toString(),
                                        StandardCharsets.UTF_8.name(),
                                        false,
                                        true));
                Enumeration<ZipArchiveEntry> entries = zip.getEntries();
                CharsetDecoder names = StandardCharsets.UTF_8.newDecoder();
                while (entries.hasMoreElements()) {
                    // Match the JDK names used by NodeManager, before Commons normalizes
                    // separators.
                    String entry =
                            names.decode(ByteBuffer.wrap(entries.nextElement().getRawName()))
                                    .toString();
                    distributionRoot = detectRoot(entry, distributionRoot, configFiles);
                }
            } else {
                InputStream input = closer.register(fileSystem.open(archive.getPath()));
                TarArchiveInputStream tar =
                        closer.register(new TarArchiveInputStream(new GZIPInputStream(input)));
                TarArchiveEntry entry;
                while ((entry = tar.getNextTarEntry()) != null) {
                    distributionRoot = detectRoot(entry.getName(), distributionRoot, configFiles);
                }
            }
        } catch (Throwable failure) {
            throw closer.rethrow(failure);
        } finally {
            closer.close();
        }
        return new YarnDistribution(archive, requireRoot(distributionRoot), configFiles);
    }

    static String archiveName(String name) {
        if (name.endsWith(".zip")) {
            return LOCALIZED_ZIP;
        }
        if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
            return LOCALIZED_TAR_GZ;
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

    private static String detectRoot(String name, String previous, Set<String> configFiles) {
        while (name.startsWith("./")) {
            name = name.substring(2);
        }
        if (name.startsWith("/")
                || name.contains("\\")
                || Arrays.asList(name.split("/")).contains("..")) {
            throw new IllegalArgumentException("Unsafe distribution archive entry: " + name);
        }
        if (!name.endsWith("/")
                && (name.startsWith(YarnConstants.CONFIG_DIRECTORY)
                        || name.contains("/" + YarnConstants.CONFIG_DIRECTORY))) {
            configFiles.add(name);
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

    public boolean containsFile(String path) {
        return configFiles.contains(root + path);
    }

    /** Persists the exact YARN resource and the runtime settings selected from this archive. */
    public void writeManifest(FileSystem fileSystem, Path target, Properties manifest)
            throws IOException {
        manifest.setProperty(ARCHIVE_PROPERTY, path.toString());
        manifest.setProperty(SIZE_PROPERTY, Long.toString(size));
        manifest.setProperty(TIMESTAMP_PROPERTY, Long.toString(timestamp));
        manifest.setProperty(ROOT_PROPERTY, root);
        try (OutputStream output = fileSystem.create(target, false)) {
            manifest.store(output, "SeaTunnel localized distribution");
        }
    }

    static Properties readManifest(FileSystem fileSystem, Path staging) throws IOException {
        Properties manifest = new Properties();
        Path path = new Path(staging, YarnConstants.DISTRIBUTION_MANIFEST);
        try (InputStream input = fileSystem.open(path)) {
            manifest.load(input);
        }
        return manifest;
    }

    /** Recreates the resource from the exact archive metadata recorded at submission. */
    static YarnDistribution fromManifest(Path staging, Properties manifest) throws IOException {
        String location = manifest.getProperty(ARCHIVE_PROPERTY);
        String root = manifest.getProperty(ROOT_PROPERTY);
        if (location == null || location.isEmpty() || root == null) {
            throw new IOException("Incomplete distribution manifest in " + staging);
        }
        try {
            return new YarnDistribution(
                    new Path(location),
                    Long.parseLong(manifest.getProperty(SIZE_PROPERTY)),
                    Long.parseLong(manifest.getProperty(TIMESTAMP_PROPERTY)),
                    root,
                    Collections.emptySet());
        } catch (NumberFormatException failure) {
            throw new IOException("Invalid distribution archive metadata in " + staging, failure);
        }
    }

    String localizedHome() {
        return YarnConstants.LOCALIZED_DISTRIBUTION_NAME + "/" + root;
    }
}
