package org.apache.seatunnel.engine.imap.storage.file.wal.writer;

import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.util.Objects;

@Getter
public class CompactionFile implements Comparable<CompactionFile> {
    private final Path path;
    private final long size;

    public CompactionFile(Path path, long size) {
        this.path = path;
        this.size = size;
    }

    @Override
    public int compareTo(CompactionFile o) {
        return Long.compare(this.size, o.size);
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        CompactionFile that = (CompactionFile) object;
        return this.size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(size);
    }
}
