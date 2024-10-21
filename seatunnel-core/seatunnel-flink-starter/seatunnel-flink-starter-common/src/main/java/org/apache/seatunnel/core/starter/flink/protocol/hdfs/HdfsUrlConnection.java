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

package org.apache.seatunnel.core.starter.flink.protocol.hdfs;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

public class HdfsUrlConnection extends URLConnection {

    private InputStream is;

    /**
     * Constructs a URL connection to the specified URL. A connection to the object referenced by
     * the URL is not created.
     *
     * @param url the specified URL.
     */
    protected HdfsUrlConnection(URL url) {
        super(url);
    }

    @Override
    public void connect() throws IOException {
        try {
            URI uri = url.toURI();
            FileSystem fs = FileSystem.get(uri);
            is = fs.open(new Path(uri));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (is == null) {
            connect();
        }
        return is;
    }
}
