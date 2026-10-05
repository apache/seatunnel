/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.transport.packet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

public class EdgePayloadCompressor {

    public static byte[] compress(byte[] data, EdgePacketCompressionType type) throws IOException {
        switch (type) {
            case NONE:
                return data;
            case GZIP:
                return gzipCompress(data);
            case ZLIB:
                return zlibCompress(data, false);
            case DEFLATE:
                return zlibCompress(data, true);
            default:
                throw new IllegalArgumentException("Unknown: " + type);
        }
    }

    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(data);
        }
        return out.toByteArray();
    }

    private static byte[] zlibCompress(byte[] data, boolean nowrap) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DeflaterOutputStream deflaterOut =
                new DeflaterOutputStream(out, new Deflater(Deflater.DEFAULT_COMPRESSION, nowrap))) {
            deflaterOut.write(data);
        }
        return out.toByteArray();
    }
}
