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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Slf4j
public final class CompressionUtils {

    private CompressionUtils() {
    }

    /**
     * Compress directory to a 'tar.gz' format file.
     *
     * @param inputDir   all files in the directory will be included, except for symbolic links.
     * @param outputFile the output tarball file.
     */
    public static void tarGzip(final Path inputDir, final Path outputFile) throws IOException {
        log.info("Tar directory '{}' to file '{}'.", inputDir, outputFile);
        try (OutputStream out = Files.newOutputStream(outputFile);
             BufferedOutputStream bufferedOut = new BufferedOutputStream(out);
             GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(bufferedOut);
             TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzOut)) {
            Files.walkFileTree(inputDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isSymbolicLink()) {
                        return FileVisitResult.CONTINUE;
                    }
                    String fileName = inputDir.relativize(path).toString();
                    TarArchiveEntry archiveEntry = new TarArchiveEntry(path.toFile(), fileName);
                    tarOut.putArchiveEntry(archiveEntry);
                    Files.copy(path, tarOut);
                    tarOut.closeArchiveEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
            tarOut.finish();
            log.info("Creating tar file '{}'.", outputFile);
        } catch (IOException e) {
            log.error("Error when tar directory '{}' to file '{}'.", inputDir, outputFile);
            throw e;
        }
    }

    /**
     * Untar an input file into an output file.
     * <p>
     * The output file is created in the output folder, having the same name
     * as the input file, minus the '.tar' extension.
     *
     * @param inputFile the input .tar file
     * @param outputDir the output directory file.
     * @throws IOException           io exception
     * @throws FileNotFoundException file not found exception
     * @throws ArchiveException      archive exception
     */
    public static void unTar(final File inputFile, final File outputDir) throws IOException, ArchiveException {

        log.info("Untaring {} to dir {}.", inputFile.getAbsolutePath(), outputDir.getAbsolutePath());

        final List<File> untaredFiles = new LinkedList<>();
        try (final InputStream is = new FileInputStream(inputFile);
             final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is)) {
            TarArchiveEntry entry = null;
            while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
                final File outputFile = new File(outputDir, entry.getName());
                if (!outputFile.toPath().normalize().startsWith(outputDir.toPath())) {
                    throw new IllegalStateException("Bad zip entry");
                }
                if (entry.isDirectory()) {
                    log.info("Attempting to write output directory {}.", outputFile.getAbsolutePath());
                    if (!outputFile.exists()) {
                        log.info("Attempting to create output directory {}.", outputFile.getAbsolutePath());
                        if (!outputFile.mkdirs()) {
                            throw new IllegalStateException(String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
                        }
                    }
                } else {
                    log.info("Creating output file {}.", outputFile.getAbsolutePath());
                    final OutputStream outputFileStream = new FileOutputStream(outputFile);
                    IOUtils.copy(debInputStream, outputFileStream);
                    outputFileStream.close();
                }
                untaredFiles.add(outputFile);
            }
        }
    }

    /**
     * Ungzip an input file into an output file.
     * <p>
     * The output file is created in the output folder, having the same name
     * as the input file, minus the '.gz' extension.
     *
     * @param inputFile the input .gz file
     * @param outputDir the output directory file.
     * @return The {@link File} with the ungzipped content.
     * @throws IOException           io exception
     * @throws FileNotFoundException file not found exception
     */
    public static File unGzip(final File inputFile, final File outputDir) throws IOException {

        log.info("Unzipping {} to dir {}.", inputFile.getAbsolutePath(), outputDir.getAbsolutePath());

        final File outputFile = new File(outputDir, inputFile.getName().substring(0, inputFile.getName().length() - 3));

        try (final FileInputStream fis = new FileInputStream(inputFile);
             final GZIPInputStream in = new GZIPInputStream(fis);
             final FileOutputStream out = new FileOutputStream(outputFile)) {
            IOUtils.copy(in, out);
        }
        return outputFile;
    }

}
