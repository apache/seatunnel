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

import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapData;
import org.apache.seatunnel.engine.imap.storage.file.common.OrcConstants;

import org.apache.commons.lang3.ClassUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        for (int i = 0; i < fieldList.size(); i++) {
            if (OrcConstants.OrcFields.DELETED.equals(fieldList.get(i).getFullFieldName())) {
                LongColumnVector longVec = (LongColumnVector) batch.cols[i];
                data.setDeleted(longVec.vector[rowNum] == 1 ? Boolean.TRUE : Boolean.FALSE);
                continue;
            }
            if (OrcConstants.OrcFields.KEY.equals(fieldList.get(i).getFullFieldName())) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                byte[] datas = bytesVector.vector[rowNum] = Arrays.copyOfRange(bytesVector.vector[rowNum], bytesVector.start[rowNum], bytesVector.start[rowNum] + bytesVector.length[rowNum]);
                data.setKey(datas);
                continue;
            }
            if (OrcConstants.OrcFields.KEY_CLASS.equals(fieldList.get(i).getFullFieldName())) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                data.setKeyClassName(bytesVector.toString(rowNum));
                continue;
            }
            if (OrcConstants.OrcFields.VALUE.equals(fieldList.get(i).getFullFieldName()) && queryAll) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                byte[] datas = bytesVector.vector[rowNum] = Arrays.copyOfRange(bytesVector.vector[rowNum], bytesVector.start[rowNum], bytesVector.start[rowNum] + bytesVector.length[rowNum]);
                data.setValue(datas);
                continue;
            }
            if (OrcConstants.OrcFields.VALUE_CLASS.equals(fieldList.get(i).getFullFieldName()) && queryAll) {
                BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[i];
                data.setValueClassName(bytesVector.toString(rowNum));
                continue;
            }
            if (OrcConstants.OrcFields.TIMESTAMP.equals(fieldList.get(i).getFullFieldName())) {
                LongColumnVector longVec = (LongColumnVector) batch.cols[i];
                data.setTimestamp(longVec.vector[rowNum]);
            }
        }
        return data;
    }

    Serializer serializer = new ProtoStuffSerializer();

    private Object deserializeData(byte[] data, String className) {
        try {
            Class<?> clazz = ClassUtils.getClass(className);
            try {
                return serializer.deserialize(data, clazz);
            } catch (IOException e) {
                //log.error("deserialize data error, data is {}, className is {}", data, className, e);
                throw new IMapStorageException(e, "deserialize data error: data is s%, className is s%", data, className);
            }
        } catch (ClassNotFoundException e) {
            //  log.error("deserialize data error, class name is {}", className, e);
            throw new IMapStorageException(e, "deserialize data error, class name is {}", className);
        }
    }
}
