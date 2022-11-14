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

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapData;
import org.apache.seatunnel.engine.imap.storage.file.common.OrcConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class OrcReader {

    /**
     * Default query batch size
     */
    private static final int DEFAULT_QUERY_LIST_SIZE = 1024;

    private Reader reader;

    private Configuration conf;

    public OrcReader(Path path, Configuration conf) throws IOException {
        this.conf = conf;
        this.reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    }

    public List<IMapData> queryAllKeys() throws IOException {
        return query(false);
    }

    private List<IMapData> query(boolean queryAll) throws IOException {
        List<IMapData> result = new ArrayList<>(DEFAULT_QUERY_LIST_SIZE);
        Reader.Options readerOptions = new Reader.Options(conf).schema(OrcConstants.DATA_SCHEMA);
        RecordReader rows = reader.rows(readerOptions);

        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        List<TypeDescription> fieldList = OrcConstants.DATA_SCHEMA.getChildren();
        while (rows.nextBatch(batch)) {
            int rowNum = 0;
            for (int i = 0; i < batch.size; i++) {
                IMapData data = convert(batch, fieldList, rowNum, queryAll);
                result.add(data);
                rowNum++;
            }
        }
        rows.close();
        return result;
    }

    public List<IMapData> queryAll() throws IOException {
        return query(true);
    }

    public void close() throws IOException {
        reader.close();
    }

    private IMapData convert(VectorizedRowBatch batch, List<TypeDescription> fieldList, int rowNum, boolean queryAll) {
        IMapData data = new IMapData();
        IntStream.range(0, fieldList.size()).forEach(i -> {
            if (OrcConstants.OrcFields.DELETED.equals(fieldList.get(i).getFullFieldName())) {
                LongColumnVector longVec = (LongColumnVector) batch.cols[i];
                data.setDeleted(longVec.vector[rowNum] == 1 ? Boolean.TRUE : Boolean.FALSE);
                return;
            }
            if (OrcConstants.OrcFields.KEY.equals(fieldList.get(i).getFullFieldName())) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                data.setKey(bytesVector.toString(rowNum).getBytes(StandardCharsets.UTF_8));
                return;
            }
            if (OrcConstants.OrcFields.KEY_CLASS.equals(fieldList.get(i).getFullFieldName())) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                data.setKeyClassName(bytesVector.toString(rowNum));
                return;
            }
            if (OrcConstants.OrcFields.VALUE.equals(fieldList.get(i).getFullFieldName()) && queryAll) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                data.setValue(bytesVector.toString(rowNum).getBytes(StandardCharsets.UTF_8));
                return;
            }
            if (OrcConstants.OrcFields.VALUE_CLASS.equals(fieldList.get(i).getFullFieldName()) && queryAll) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                data.setValueClassName(bytesVector.toString(rowNum));
                return;
            }
            if (OrcConstants.OrcFields.TIMESTAMP.equals(fieldList.get(i).getFullFieldName())) {
                LongColumnVector longVec = (LongColumnVector) batch.cols[i];
                data.setTimestamp(longVec.vector[rowNum]);
            }
        });
        return data;
    }
}
