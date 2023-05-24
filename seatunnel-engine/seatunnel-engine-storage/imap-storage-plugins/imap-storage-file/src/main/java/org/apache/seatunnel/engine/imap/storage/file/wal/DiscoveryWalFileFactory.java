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
package org.apache.seatunnel.engine.imap.storage.file.wal;

import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.DefaultReader;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.IFileReader;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.HdfsWriter;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.OssWriter;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.S3Writer;

public class DiscoveryWalFileFactory {

    public static IFileReader getReader(String type) {
        FileConfiguration configuration = FileConfiguration.valueOf(type.toUpperCase());
        switch (configuration) {
            case HDFS:
            case S3:
            case OSS:
                return new DefaultReader();
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    public static IFileWriter getWriter(String type) {
        FileConfiguration configuration = FileConfiguration.valueOf(type.toUpperCase());
        switch (configuration) {
            case HDFS:
                return new HdfsWriter();
            case S3:
                return new S3Writer();
            case OSS:
                return new OssWriter();
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }
}
