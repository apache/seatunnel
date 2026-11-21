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
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Groups complete ORC stripes by compressed byte size; never divides a stripe. */
public class OrcFileSplitStrategy implements FileSplitStrategy, Closeable {
    private final long splitSizeBytes;
    private final HadoopFileSystemProxy fileSystem;

    public OrcFileSplitStrategy(long splitSizeBytes, HadoopConf hadoopConf) {
        if (splitSizeBytes <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    "SplitSizeBytes must be greater than 0");
        }
        this.splitSizeBytes = splitSizeBytes;
        this.fileSystem = new HadoopFileSystemProxy(hadoopConf);
    }

    /**
     * Reads stripe metadata with the configured filesystem and Hadoop authentication, closing the
     * metadata reader before returning. The filesystem remains owned by this strategy until close.
     * Failures retain the file path, table identifier and original cause.
     */
    @Override
    public List<FileSourceSplit> split(String tableId, String filePath) {
        try {
            return fileSystem.doWithHadoopAuth(
                    (configuration, user) -> {
                        try (Reader reader =
                                OrcFile.createReader(
                                        new Path(filePath),
                                        OrcFile.readerOptions(configuration)
                                                .filesystem(fileSystem.getFileSystem()))) {
                            return splitByStripes(tableId, filePath, reader.getStripes());
                        }
                    });
        } catch (Exception exception) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_FAIL,
                    String.format(
                            "Failed to split ORC file [%s] for table [%s]", filePath, tableId),
                    exception);
        }
    }

    /**
     * Groups stripes in file order into contiguous byte ranges without dividing a stripe. The
     * configured size is a target: an oversized stripe occupies one split, while an empty stripe list
     * produces no splits. Each range starts at its first stripe and ends after its last stripe.
     */
    List<FileSourceSplit> splitByStripes(
            String tableId, String path, List<StripeInformation> stripes) {
        List<FileSourceSplit> splits = new ArrayList<>();
        long start = -1;
        long end = -1;
        for (StripeInformation stripe : stripes) {
            long stripeStart = stripe.getOffset();
            long stripeEnd = stripeStart + stripe.getLength();
            if (start >= 0 && stripeEnd - start > splitSizeBytes) {
                splits.add(new FileSourceSplit(tableId, path, start, end - start));
                start = -1;
            }
            if (start < 0) {
                start = stripeStart;
            }
            end = stripeEnd;
        }
        if (start >= 0) {
            splits.add(new FileSourceSplit(tableId, path, start, end - start));
        }
        return splits;
    }

    /** Closes the filesystem owned by this strategy after split enumeration is finished. */
    @Override
    public void close() throws IOException {
        fileSystem.close();
    }
}
