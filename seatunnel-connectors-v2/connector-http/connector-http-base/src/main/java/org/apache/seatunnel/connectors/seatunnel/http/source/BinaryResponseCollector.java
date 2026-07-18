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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;

public class BinaryResponseCollector {

    public static final SeaTunnelRowType BINARY_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"data", "relativePath", "partIndex"},
                    new SeaTunnelDataType[] {
                        PrimitiveByteArrayType.INSTANCE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                    });

    private BinaryResponseCollector() {}

    public static void collect(
            HttpResponse response, String url, long chunkSize, Collector<SeaTunnelRow> output) {
        String filename = FilenameExtractor.extract(response.getContentDisposition(), url);

        byte[] bodyBytes = response.getBodyBytes();
        if (bodyBytes == null || bodyBytes.length == 0) {
            return;
        }

        if (chunkSize <= 0 || bodyBytes.length <= chunkSize) {
            output.collect(new SeaTunnelRow(new Object[] {bodyBytes, filename, 0L}));
            return;
        }

        int offset = 0;
        long partIndex = 0;
        while (offset < bodyBytes.length) {
            int end = Math.min(offset + (int) chunkSize, bodyBytes.length);
            byte[] chunk = new byte[end - offset];
            System.arraycopy(bodyBytes, offset, chunk, 0, end - offset);
            output.collect(new SeaTunnelRow(new Object[] {chunk, filename, partIndex}));
            offset = end;
            partIndex++;
        }
    }
}
