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

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils.WAL_DATA_METADATA_LENGTH;

public class WALFileIterator implements IMapFileIterator {

    private final FSDataInputStream in;
    private final Serializer serializer;

    public WALFileIterator(FSDataInputStream in, Serializer serializer) {
        this.in = in;
        this.serializer = serializer;
    }

    @Override
    public boolean hasNext() throws IOException {
        return in.available() > WAL_DATA_METADATA_LENGTH;
    }

    @Override
    public IMapFileData next() throws IOException {
        byte[] meta = new byte[WAL_DATA_METADATA_LENGTH];
        in.readFully(meta);
        int len = WALDataUtils.byteArrayToInt(meta);

        byte[] data = new byte[len];
        in.readFully(data);

        return serializer.deserialize(data, IMapFileData.class);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
