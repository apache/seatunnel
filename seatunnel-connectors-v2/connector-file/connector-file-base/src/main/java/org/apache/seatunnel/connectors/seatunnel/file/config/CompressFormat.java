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

import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.Serializable;

public enum CompressFormat implements Serializable {
    // text json orc parquet support
    LZO(".lzo", CompressionKind.LZO, CompressionCodecName.LZO),

    // orc and parquet support
    NONE("", CompressionKind.NONE, CompressionCodecName.UNCOMPRESSED),
    SNAPPY(".snappy", CompressionKind.SNAPPY, CompressionCodecName.SNAPPY),
    LZ4(".lz4", CompressionKind.LZ4, CompressionCodecName.LZ4),

    // only orc support
    ZLIB(".zlib", CompressionKind.ZLIB, CompressionCodecName.UNCOMPRESSED),

    // only parquet support
    GZIP(".gz", CompressionKind.NONE, CompressionCodecName.GZIP),
    BROTLI(".br", CompressionKind.NONE, CompressionCodecName.BROTLI),
    ZSTD(".zstd", CompressionKind.NONE, CompressionCodecName.ZSTD);

    private final String compressCodec;
    private final CompressionKind orcCompression;
    private final CompressionCodecName parquetCompression;

    CompressFormat(
            String compressCodec,
            CompressionKind orcCompression,
            CompressionCodecName parentCompression) {
        this.compressCodec = compressCodec;
        this.orcCompression = orcCompression;
        this.parquetCompression = parentCompression;
    }

    public String getCompressCodec() {
        return compressCodec;
    }

    public CompressionKind getOrcCompression() {
        return orcCompression;
    }

    public CompressionCodecName getParquetCompression() {
        return parquetCompression;
    }
}
