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

package org.apache.seatunnel.connectors.seatunnel.file.config;

/**
 * ZIP etc.:
 *
 * <p>Archive format: ZIP can compress multiple files and directories into a single archive.
 *
 * <p><br>
 * Gzip etc.:
 *
 * <p>Single file compression: Gzip compresses only one file at a time, without creating an archive.
 *
 * <p><br>
 * Distinction: {@link org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat}
 */
public enum ArchiveCompressFormat {
    NONE(""),
    ZIP(".zip"),
    TAR(".tar"),
    TAR_GZ(".tar.gz"),
    GZ(".gz"),
    ;
    private final String archiveCompressCodec;

    ArchiveCompressFormat(String archiveCompressCodec) {
        this.archiveCompressCodec = archiveCompressCodec;
    }

    public String getArchiveCompressCodec() {
        return archiveCompressCodec;
    }
}
