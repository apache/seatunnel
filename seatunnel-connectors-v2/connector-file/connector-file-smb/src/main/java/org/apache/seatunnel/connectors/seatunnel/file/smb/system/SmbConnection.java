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

package org.apache.seatunnel.connectors.seatunnel.file.smb.system;

import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;

import java.io.Closeable;
import java.io.IOException;

public class SmbConnection implements Closeable {

    private final Connection connection;
    private final Session session;
    private final DiskShare diskShare;

    SmbConnection(Connection connection, Session session, DiskShare diskShare) {
        this.connection = connection;
        this.session = session;
        this.diskShare = diskShare;
    }

    public DiskShare getDiskShare() {
        return diskShare;
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            if (diskShare != null) {
                diskShare.close();
            }
        } catch (Exception e) {
            failure = new IOException(e);
        }
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            if (failure == null) {
                failure = new IOException(e);
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            if (failure == null) {
                failure = new IOException(e);
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
