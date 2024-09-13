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
