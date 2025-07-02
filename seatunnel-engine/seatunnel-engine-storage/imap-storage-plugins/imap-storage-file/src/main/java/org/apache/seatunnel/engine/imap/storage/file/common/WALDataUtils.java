/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class WALDataUtils {

    public static final int WAL_DATA_METADATA_LENGTH = 12;
    public static final String FILE_NAME = "wal.txt";
    public static final String PROGRESSING_SUFFIX = ".progressing";

    public static byte[] wrapperBytes(byte[] bytes) {
        byte[] metadata = new byte[WAL_DATA_METADATA_LENGTH];
        byte[] length = intToByteArray(bytes.length);
        System.arraycopy(length, 0, metadata, 0, length.length);
        byte[] result = new byte[bytes.length + WAL_DATA_METADATA_LENGTH];
        System.arraycopy(metadata, 0, result, 0, metadata.length);
        System.arraycopy(bytes, 0, result, metadata.length, bytes.length);
        return result;
    }

    public static int byteArrayToInt(byte[] encodedValue) {
        int value = (encodedValue[3] << (Byte.SIZE * 3));
        value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
        value |= (encodedValue[1] & 0xFF) << (Byte.SIZE);
        value |= (encodedValue[0] & 0xFF);
        return value;
    }

    public static byte[] intToByteArray(int value) {
        byte[] encodedValue = new byte[Integer.SIZE / Byte.SIZE];
        encodedValue[3] = (byte) (value >> Byte.SIZE * 3);
        encodedValue[2] = (byte) (value >> Byte.SIZE * 2);
        encodedValue[1] = (byte) (value >> Byte.SIZE);
        encodedValue[0] = (byte) value;
        return encodedValue;
    }

    /**
     * return direct data file names of the specified dir
     *
     * @param fs target file system
     * @param parentPath parent dir
     * @return file names
     */
    public static List<String> getDataFiles(FileSystem fs, Path parentPath, String suffix) {
        try {
            if (!fs.exists(parentPath)) {
                return new ArrayList<>();
            }
            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator =
                    fs.listLocatedStatus(parentPath);
            List<String> fileNames = new ArrayList<>();
            while (fileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
                if (fileStatus.isDirectory()) continue;
                if (fileStatus.getPath().getName().endsWith(suffix)) {
                    fileNames.add(fileStatus.getPath().toString());
                }
            }
            return fileNames;
        } catch (IOException e) {
            throw new IMapStorageException(e, "get file names error,path is s%", parentPath);
        }
    }

    /** open files by filenames, and compose InputStream in order as SequenceInputStream */
    public static SequenceInputStream getComposedInputStream(FileSystem fs, List<String> filenames)
            throws IOException {
        List<Path> paths = filenames.stream().map(Path::new).collect(Collectors.toList());
        // get file streams
        List<FSDataInputStream> streams =
                paths.stream()
                        .sorted(Comparator.comparing(Path::getName)) // sort Path by filename asc
                        .map(
                                path -> {
                                    try {
                                        return fs.open(path);
                                    } catch (IOException e) {
                                        throw new IMapStorageException(e);
                                    }
                                }) // open files
                        .collect(Collectors.toList());
        return new SequenceInputStream(Collections.enumeration(streams));
    }

    public static byte[] readNextData(SequenceInputStream stream) throws IOException {
        // read metadata
        byte[] metadata = new byte[WAL_DATA_METADATA_LENGTH];
        int readBytes = 0;
        while (readBytes != WAL_DATA_METADATA_LENGTH) {
            int read = stream.read(metadata, readBytes, metadata.length - readBytes);
            if (read == -1) return null;
            readBytes += read;
        }

        // read data entry
        int dataLen = WALDataUtils.byteArrayToInt(metadata);
        ByteArrayOutputStream out = new ByteArrayOutputStream(dataLen);
        readBytes = 0;
        byte[] buffer = new byte[1024];
        while (readBytes != dataLen) {
            int len = Math.min(dataLen - readBytes, buffer.length);
            int read = stream.read(buffer, 0, len);
            if (read == -1) return null;
            readBytes += read;
            out.write(buffer, 0, read);
        }

        return out.toByteArray();
    }
}
