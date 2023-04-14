package org.apache.seatunnel.connectors.seatunnel.mongodb.state;

import org.bson.Document;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DocumentBulk is buffered {@link Document} in memory, which would be written to MongoDB in a
 * single transaction. Due to execution efficiency, each DocumentBulk maybe be limited to a maximum
 * size, typically 1,000 documents. But for the transactional mode, the maximum size should not be
 * respected because all that data must be written in one transaction.
 */
@NotThreadSafe
@ToString
@EqualsAndHashCode
public class DocumentBulk implements Serializable {

    private final List<Document> bufferedDocuments;

    private final long maxSize;

    private static final int BUFFER_INIT_SIZE = Integer.MAX_VALUE;

    public DocumentBulk(long maxSize) {
        this.maxSize = maxSize;
        bufferedDocuments = new ArrayList<>(1024);
    }

    DocumentBulk() {
        this(BUFFER_INIT_SIZE);
    }

    public void add(Document document) {
        if (bufferedDocuments.size() == maxSize) {
            throw new IllegalStateException("DocumentBulk is already full");
        }
        bufferedDocuments.add(document);
    }

    public int size() {
        return bufferedDocuments.size();
    }

    boolean isFull() {
        return bufferedDocuments.size() >= maxSize;
    }

    public List<Document> getDocuments() {
        return bufferedDocuments;
    }
}
