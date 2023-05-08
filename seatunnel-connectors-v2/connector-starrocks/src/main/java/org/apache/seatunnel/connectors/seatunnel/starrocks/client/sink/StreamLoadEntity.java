package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Slf4j
public class StreamLoadEntity extends AbstractHttpEntity {

    protected static final int OUTPUT_BUFFER_SIZE = 2048;

    private static final Header CONTENT_TYPE =
            new BasicHeader(
                    HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.toString());

    private final TableRegion region;
    private final InputStream content;

    private final boolean chunked;
    private final long contentLength;

    public StreamLoadEntity(
            TableRegion region, StreamLoadDataFormat dataFormat, StreamLoadEntityMeta meta) {
        this.region = region;
        this.content = new StreamLoadStream(region, dataFormat);
        this.chunked = meta.getBytes() == -1L;
        this.contentLength = meta.getBytes();
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public boolean isChunked() {
        return chunked;
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public Header getContentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Header getContentEncoding() {
        return null;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return content;
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        long total = 0;
        try (InputStream inputStream = this.content) {
            final byte[] buffer = new byte[OUTPUT_BUFFER_SIZE];
            int l;
            while ((l = inputStream.read(buffer)) != -1) {
                total += l;
                outputStream.write(buffer, 0, l);
            }
        }
        log.info("Entity write end, contentLength : {}, total : {}", contentLength, total);
    }

    @Override
    public boolean isStreaming() {
        return true;
    }
}
