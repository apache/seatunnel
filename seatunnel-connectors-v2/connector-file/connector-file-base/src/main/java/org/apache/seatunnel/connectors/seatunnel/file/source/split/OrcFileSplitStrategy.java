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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link OrcFileSplitStrategy} defines a split strategy for ORC files based on ORC stripes.
 *
 * <p>This strategy uses {@code Stripe} as the minimum indivisible split unit and generates {@link
 * FileSourceSplit}s by merging one or more contiguous stripes according to the configured split
 * size. A split will never break a stripe, ensuring correctness and compatibility with ORC readers.
 *
 * <p>The generated split range ({@code start}, {@code length}) represents a byte range covering
 * complete stripes. The actual row-level reading and decoding are delegated to the ORC reader
 * implementation.
 */
public class OrcFileSplitStrategy implements FileSplitStrategy {

    private final long splitSizeBytes;

    public OrcFileSplitStrategy(long splitSizeBytes) {
        if (splitSizeBytes <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    "SplitSizeBytes must be greater than 0");
        }
        this.splitSizeBytes = splitSizeBytes;
    }

    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        try {
            return splitByStripes(tableId, filePath, readStripes(filePath));
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(FileConnectorErrorCode.FILE_SPLIT_FAIL, e);
        }
    }

    /** Core split logic based on stripe metadata. This method is IO-free and unit-test friendly. */
    List<FileSourceSplit> splitByStripes(
            String tableId, String filePath, List<StripeInformation> stripes) {
        List<FileSourceSplit> splits = new ArrayList<>();
        if (stripes == null || stripes.isEmpty()) {
            return splits;
        }
        long currentStart = 0;
        long currentLength = 0;
        boolean hasOpenSplit = false;
        for (StripeInformation stripe : stripes) {
            long stripeStart = stripe.getOffset();
            long stripeSize = stripe.getLength();
            if (!hasOpenSplit) {
                currentStart = stripeStart;
                currentLength = stripeSize;
                hasOpenSplit = true;
                continue;
            }
            if (currentLength + stripeSize > splitSizeBytes) {
                splits.add(new FileSourceSplit(tableId, filePath, currentStart, currentLength));
                currentStart = stripeStart;
                currentLength = stripeSize;
            } else {
                currentLength += stripeSize;
            }
        }
        if (hasOpenSplit && currentLength > 0) {
            splits.add(new FileSourceSplit(tableId, filePath, currentStart, currentLength));
        }
        return splits;
    }

    private List<StripeInformation> readStripes(String filePath) throws IOException {
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        try (Reader reader = OrcFile.createReader(path, readerOptions)) {
            return reader.getStripes();
        }
    }
}
