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

package org.apache.seatunnel.connectors.tailfile.source.tailfile;

import org.apache.seatunnel.connectors.tailfile.source.TailFileSourceConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Getter
@Setter
@Slf4j
public class FileHarvester implements Closeable {
    // \n
    private static final byte BYTE_NL = (byte) 10;
    // \r
    private static final byte BYTE_CR = (byte) 13;
    private static final int NEED_READING = -1;

    private final TailFileSourceConfig config;
    private final File file;
    private final String path;
    private final String parentName;
    private final long inode;
    private boolean lineEnd;
    private Pattern multilinePattern;

    private RandomAccessFile raf;
    // todo 删除
    private long fileStartPos;
    private long linePos;
    private long lastUpdated;
    private boolean needTail;

    private byte[] segmentBuffer;
    private int segmentBufferPos;
    private byte[] lineBuffer;

    public FileHarvester(File file, long inode, long pos, TailFileSourceConfig config)
            throws IOException {
        this(file, inode, pos, pos == 0, config);
    }

    public FileHarvester(
            File file, long inode, long pos, boolean lineEnd, TailFileSourceConfig config)
            throws IOException {
        this.config = config;
        this.file = file;
        this.path = file.getAbsolutePath();
        this.parentName = file.getParentFile().getName();
        this.inode = inode;
        this.lineEnd = lineEnd;
        this.multilinePattern =
                config.getMultilinePattern() != null
                        ? Pattern.compile(config.getMultilinePattern())
                        : null;

        this.raf = new RandomAccessFile(file, "r");
        this.fileStartPos = pos;
        if (fileStartPos > 0) {
            raf.seek(fileStartPos);
            linePos = fileStartPos;
        }

        this.lastUpdated = 0L;
        this.needTail = true;
        this.lineBuffer = new byte[0];
        this.segmentBufferPos = NEED_READING;
    }

    private void setNextLineStartPos(long pos) {
        linePos = pos;
    }

    public boolean resetPos(String path, long inode, long pos) throws IOException {
        if (this.inode == inode && this.path.equals(path)) {
            setFileStartPos(pos);
            setLineEnd(pos == 0);
            resetFilePos(pos);
            log.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
            return true;
        }
        return false;
    }

    private void resetFilePos(long pos) throws IOException {
        raf.seek(pos);
        linePos = pos;
        segmentBufferPos = NEED_READING;
        lineBuffer = new byte[0];
    }

    public int tail(int batchSize, Consumer<FileRowEvent> consumer) throws IOException {
        if (multilinePattern != null) {
            return tailMultiLineEvent(batchSize, consumer);
        }
        return tailSingleLineEvent(batchSize, consumer);
    }

    private int tailSingleLineEvent(int batchSize, Consumer<FileRowEvent> consumer)
            throws IOException {
        int rowCount;
        for (rowCount = 0; rowCount < batchSize; ) {
            FileRowEvent event = tailLineEvent();
            if (event == null) {
                break;
            }

            if (!lineEnd && event.isEnd() && event.getBody().isEmpty()) {
                log.debug(
                        "Skip empty line, path: {}, inode: {}, pos: {}",
                        path,
                        inode,
                        event.getPos());
                lineEnd = event.isEnd();
                continue;
            }

            rowCount++;
            consumer.accept(event);
            lineEnd = event.isEnd();
        }
        return rowCount;
    }

    private int tailMultiLineEvent(int batchSize, Consumer<FileRowEvent> consumer)
            throws IOException {
        long rowStartTime = System.currentTimeMillis();
        long rowStartPos = getLinePos();
        int rowCount = 0;

        FileRowEvent prevRowEvent = null;
        boolean multiLineEnd = this.lineEnd;
        while (rowCount < batchSize) {
            long startTime = System.currentTimeMillis();
            FileRowEvent nextRowEvent = tailLineEvent();
            if (nextRowEvent == null) {
                break;
            }
            if (!multiLineEnd && nextRowEvent.isEnd() && nextRowEvent.getBody().isEmpty()) {
                log.debug(
                        "Skip empty line, path: {}, inode: {}, pos: {}",
                        path,
                        inode,
                        nextRowEvent.getPos());
                multiLineEnd = nextRowEvent.isEnd();
                continue;
            }

            multiLineEnd = nextRowEvent.isEnd();
            if (prevRowEvent == null) {
                prevRowEvent = nextRowEvent;
            } else if (multilinePattern.matcher(nextRowEvent.getBody()).find()) {
                consumer.accept(prevRowEvent);
                lineEnd = prevRowEvent.isEnd();
                prevRowEvent = nextRowEvent;
                rowCount++;
                rowStartPos = nextRowEvent.getPos();
                rowStartTime = startTime;
            } else {
                String separator = prevRowEvent.getSeparator();
                if (separator == null) {
                    // If separator is null, it means the previous line is a single line
                    separator = "";
                }
                prevRowEvent.mergeMultiline(
                        separator, nextRowEvent, config.getMaxMessageBytes(), config.getCharset());
            }
        }

        if (prevRowEvent != null) {
            long lastModified = file.lastModified();
            if (rowStartTime > lastModified
                    && rowStartTime - lastModified > config.getLineTimeout()) {
                log.debug(
                        "Line wait {}ms timed out, send up unclosed line data, path: {}, inode: {}, pos: {}, len: {}",
                        config.getLineTimeout(),
                        path,
                        inode,
                        rowStartPos,
                        prevRowEvent.getBody().length());
                consumer.accept(prevRowEvent);
                rowCount++;
                lineEnd = prevRowEvent.isEnd();
            } else {
                log.debug(
                        "Backing off in file without newline: {}, inode: {}, pos: {}",
                        path,
                        inode,
                        raf.getFilePointer());
                resetFilePos(rowStartPos);
            }
        }

        return rowCount;
    }

    private FileRowEvent tailLineEvent() throws IOException {
        long now = System.currentTimeMillis();
        long lineStartPos = getLinePos();
        Line line = readLine();
        if (line == null) {
            return null;
        }

        if (Boolean.FALSE.equals(line.isEnd())) {
            long lastModified = file.lastModified();
            if (now > lastModified && now - lastModified > config.getLineTimeout()) {
                // If the line is not ended and the file has not been modified for a while, it
                // indicates that it is the last line of file and send the line-up
                log.debug(
                        "Line wait {}ms timed out, send up unclosed line data, path: {}, inode: {}, pos: {}, len: {}",
                        config.getLineTimeout(),
                        path,
                        inode,
                        lineStartPos,
                        line.getData().length);
            } else {
                // An incomplete half-row of data was read
                log.debug(
                        "Backing off in file without newline: {}, inode: {}, pos: {}",
                        path,
                        inode,
                        raf.getFilePointer());
                resetFilePos(lineStartPos);
                return null;
            }
        }
        return new FileRowEvent(
                lineStartPos,
                line.getStr(),
                line.isEnd(),
                line.getSeparator(),
                line.getOriginalLength(),
                line.isTruncated());
    }

    private Line readLine() throws IOException {
        Line line = null;
        int lineBufferOriginalLen = 0;
        while (true) {
            if (segmentBufferPos == NEED_READING) {
                if (raf.getFilePointer() < raf.length()) {
                    readFileToSegmentBuffer();
                } else {
                    if (lineBuffer.length > 0) {
                        line = new Line(lineBuffer, false, null, lineBufferOriginalLen);
                        lineBuffer = new byte[0];
                        setNextLineStartPos(linePos + lineBufferOriginalLen);
                    }
                    break;
                }
            }
            for (int index = segmentBufferPos; index < segmentBuffer.length; index++) {
                if (segmentBuffer[index] == BYTE_NL) {
                    String separator = String.valueOf(((char) BYTE_NL));
                    int lineBufferValidLen = lineBuffer.length;
                    // Don't copy last byte(NEW_LINE)
                    int segmentBufferValidLen = index - segmentBufferPos;
                    // For windows, check for CR
                    if (index > 0 && segmentBuffer[index - 1] == BYTE_CR) {
                        segmentBufferValidLen -= 1;
                        separator = String.valueOf(((char) BYTE_CR)) + (char) BYTE_NL;
                    } else if (lineBufferValidLen > 0
                            && lineBuffer[lineBufferValidLen - 1] == BYTE_CR) {
                        lineBufferValidLen -= 1;
                        separator = String.valueOf(((char) BYTE_CR)) + (char) BYTE_NL;
                    }

                    line =
                            new Line(
                                    concatByteArrays(
                                            lineBuffer,
                                            0,
                                            lineBufferValidLen,
                                            segmentBuffer,
                                            segmentBufferPos,
                                            segmentBufferValidLen),
                                    true,
                                    separator,
                                    lineBufferOriginalLen);
                    setNextLineStartPos(
                            linePos + (lineBufferOriginalLen + (index - segmentBufferPos + 1)));

                    lineBuffer = new byte[0];
                    if (index + 1 < segmentBuffer.length) {
                        segmentBufferPos = index + 1;
                    } else {
                        segmentBufferPos = NEED_READING;
                    }
                    break;
                }
            }
            if (line != null) {
                break;
            }

            // Cache the segment buffer into line buffer
            if (lineBuffer.length < config.getMaxMessageBytes()) {
                lineBuffer =
                        concatByteArrays(
                                lineBuffer,
                                0,
                                lineBuffer.length,
                                segmentBuffer,
                                segmentBufferPos,
                                (segmentBuffer.length - segmentBufferPos));
                lineBufferOriginalLen = lineBuffer.length;
            } else {
                lineBufferOriginalLen += (segmentBuffer.length - segmentBufferPos);
            }
            segmentBufferPos = NEED_READING;
        }
        return line;
    }

    private void readFileToSegmentBuffer() throws IOException {
        if ((raf.length() - raf.getFilePointer()) < config.getBufferSize()) {
            segmentBuffer = new byte[(int) (raf.length() - raf.getFilePointer())];
        } else {
            segmentBuffer = new byte[config.getBufferSize()];
        }
        raf.read(segmentBuffer, 0, segmentBuffer.length);
        segmentBufferPos = 0;
    }

    private byte[] concatByteArrays(
            byte[] a, int startIdxA, int readLenA, byte[] b, int startIdxB, int readLenB) {
        byte[] c = new byte[readLenA + readLenB];
        System.arraycopy(a, startIdxA, c, 0, readLenA);
        System.arraycopy(b, startIdxB, c, readLenA, readLenB);
        return c;
    }

    @Override
    public void close() {
        if (raf == null) {
            return;
        }
        try {
            raf.close();
            raf = null;
            long now = System.currentTimeMillis();
            setLastUpdated(now);
            // todo 加日志
        } catch (Exception e) {
            log.error("Failed closing file: " + path + ", inode: " + inode, e);
        }
    }

    @Getter
    @AllArgsConstructor
    private class Line {
        private final byte[] data;
        private final boolean end;
        private final String separator;
        private final int originalLength;
        private final boolean truncated;
        private String str;

        public Line(byte[] data, boolean end, String separator, int originalLength) {
            this.originalLength = originalLength;
            if (data.length > config.getMaxMessageBytes()) {
                this.truncated = true;
                data = Arrays.copyOf(data, config.getMaxMessageBytes());
            } else {
                this.truncated = originalLength > data.length;
            }
            this.data = data;
            this.end = end;
            this.separator = separator;
        }

        public String getStr() throws UnsupportedEncodingException {
            if (str == null) {
                str = new String(data, config.getCharset());
            }
            return str;
        }
    }
}
