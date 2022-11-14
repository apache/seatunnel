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

package org.apache.seatunnel.engine.imap.storage.file.orc;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.imap.storage.file.common.OrcConstants;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class OrcWriter implements AutoCloseable {

    private Writer writer;

    VectorizedRowBatch batch = OrcConstants.DATA_SCHEMA.createRowBatch(2);

    LongColumnVector deleted = (LongColumnVector) batch.cols[OrcConstants.OrcFields.DELETED_INDEX];
    BytesColumnVector key = (BytesColumnVector) batch.cols[OrcConstants.OrcFields.KEY_INDEX];
    BytesColumnVector keyClassName = (BytesColumnVector) batch.cols[OrcConstants.OrcFields.KEY_CLASS_INDEX];
    BytesColumnVector value = (BytesColumnVector) batch.cols[OrcConstants.OrcFields.VALUE_INDEX];
    BytesColumnVector valueClassName = (BytesColumnVector) batch.cols[OrcConstants.OrcFields.VALUE_CLASS_INDEX];
    LongColumnVector timestamp = (LongColumnVector) batch.cols[OrcConstants.OrcFields.TIMESTAMP_INDEX];

    private FileSystem fs;

    private Path currentPath;

    public OrcWriter(Path path, Configuration conf) throws IOException {
        //todo need improve, stripe size is so small
        this.writer =
            OrcFile.createWriter(path,
                  OrcFile.writerOptions(conf).setSchema(OrcConstants.DATA_SCHEMA).stripeSize(1));
        this.fs = path.getFileSystem(conf);
        this.currentPath = path;
    }

    public void write(IMapFileData iMapFileData) throws IOException {
        log.debug("write data to orc file" + iMapFileData.getTimestamp());
        int row = batch.size++;
        deleted.vector[row] = iMapFileData.isDeleted() ? 1 : 0;
        key.setVal(row, iMapFileData.getKey());
        keyClassName.setVal(row, iMapFileData.getKeyClassName().getBytes(StandardCharsets.UTF_8));
        value.setVal(row, iMapFileData.getValue());
        valueClassName.setVal(row, iMapFileData.getValueClassName().getBytes(StandardCharsets.UTF_8));
        timestamp.vector[row] = iMapFileData.getTimestamp();

        if (batch.size == batch.getMaxSize()) {
            try {
                writer.addRowBatch(batch);
            } catch (IOException e) {
                log.error("write orc file error, walEventBean is:{} ", iMapFileData, e);
            }
            batch.reset();
            writer.writeIntermediateFooter();
        }
    }

    @Override
    public void close() {
        if (batch.size != 0) {
            try {
                writer.addRowBatch(batch);
            } catch (IOException e) {
                e.printStackTrace();
            }
            batch.reset();
        }
        log.info("orc writer close");
        try {
            writer.close();
        } catch (IOException e) {
            log.error("close orc writer error", e);
        }
    }

    public void archive(String parentPath) throws IOException {
        writer.close();
        log.info("archive orc file, current path is:{}", currentPath);
        Path archivePath = new Path(parentPath + FileConstants.DEFAULT_IMAP_FILE_PATH_SPLIT + OrcConstants.ORC_FILE_ARCHIVE_PATH
            + FileConstants.DEFAULT_IMAP_FILE_PATH_SPLIT + OrcConstants.ORC_FILE_ARCHIVE_PATH + System.nanoTime() + OrcConstants.ORC_FILE_SUFFIX);
        fs.rename(currentPath, archivePath);
        log.info("archive orc file success, archive path is:{}", archivePath);
    }
}
