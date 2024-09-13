package org.apache.seatunnel.core.starter.flink.protocol.hdfs;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler {
    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        return new HdfsUrlConnection(u);
    }
}
