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

package org.apache.seatunnel.flink.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DorisDynamicOutputFormat
 **/
public class DorisOutputFormat<T> extends RichOutputFormat<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DorisOutputFormat.class);
    private static final long serialVersionUID = -4514164348993670086L;
    private static final long DEFAULT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String FIELD_DELIMITER_KEY = "column_separator";
    private static final String FIELD_DELIMITER_DEFAULT = "\t";
    private static final String LINE_DELIMITER_KEY = "line_delimiter";
    private static final String LINE_DELIMITER_DEFAULT = "\n";
    private static final String FORMAT_KEY = "format";
    private static final String FORMAT_JSON_VALUE = "json";
    private static final String NULL_VALUE = "\\N";
    private static final String ESCAPE_DELIMITERS_KEY = "escape_delimiters";
    private static final String ESCAPE_DELIMITERS_DEFAULT = "false";
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("\\\\x(\\d{2})");
    private final String[] fieldNames;
    private final boolean jsonFormat;
    private final int batchSize;
    private final int maxRetries;
    private final long batchIntervalMs;
    private final List batch = new ArrayList<>();
    private String fieldDelimiter;
    private String lineDelimiter;
    private DorisStreamLoad dorisStreamLoad;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;
    private transient volatile boolean closed = false;

    public DorisOutputFormat(DorisStreamLoad dorisStreamLoad,
                             String[] fieldNames,
                             int batchSize, long batchIntervalMs, int maxRetries) {
        this.dorisStreamLoad = dorisStreamLoad;
        parseDelimiter();
        this.fieldNames = fieldNames;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
        this.jsonFormat = FORMAT_JSON_VALUE.equals(dorisStreamLoad.getStreamLoadProp().getProperty(FORMAT_KEY));
    }

    private void parseDelimiter() {
        Properties streamLoadProp = dorisStreamLoad.getStreamLoadProp();
        boolean ifEscape = Boolean.parseBoolean(streamLoadProp.getProperty(ESCAPE_DELIMITERS_KEY, ESCAPE_DELIMITERS_DEFAULT));
        if (ifEscape) {
            this.fieldDelimiter = escapeString(streamLoadProp.getProperty(FIELD_DELIMITER_KEY,
                    FIELD_DELIMITER_DEFAULT));
            this.lineDelimiter = escapeString(streamLoadProp.getProperty(LINE_DELIMITER_KEY,
                    LINE_DELIMITER_DEFAULT));

            if (streamLoadProp.contains(ESCAPE_DELIMITERS_KEY)) {
                streamLoadProp.remove(ESCAPE_DELIMITERS_KEY);
            }
        } else {
            this.fieldDelimiter = streamLoadProp.getProperty(FIELD_DELIMITER_KEY,
                    FIELD_DELIMITER_DEFAULT);
            this.lineDelimiter = streamLoadProp.getProperty(LINE_DELIMITER_KEY,
                    LINE_DELIMITER_DEFAULT);
        }
    }

    private String escapeString(String s) {
        Matcher m = DELIMITER_PATTERN.matcher(s);
        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(buf, String.format("%s", (char) Integer.parseInt(m.group(1))));
        }
        m.appendTail(buf);
        return buf.toString();
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) {
        if (batchIntervalMs > 0 && batchSize != 1) {
            this.scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("doris-streamload-outputformat"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (DorisOutputFormat.this) {
                    if (!closed) {
                        try {
                            flush();
                        } catch (Exception e) {
                            flushException = e;
                        }
                    }
                }
            }, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to streamload failed.", flushException);
        }
    }

    @Override
    public synchronized void writeRecord(T row) throws IOException {
        checkFlushException();
        addBatch(row);
        if (batchSize > 0 && batch.size() >= batchSize) {
            flush();
        }
    }

    private void addBatch(T row) {
        if (row instanceof Row) {
            Row rowData = (Row) row;
            Map<String, String> valueMap = Maps.newHashMap();
            StringJoiner value = new StringJoiner(this.fieldDelimiter);
            for (int i = 0; i < rowData.getArity(); ++i) {
                Object field = rowData.getField(i);
                if (jsonFormat) {
                    String data = field != null ? field.toString() : null;
                    valueMap.put(this.fieldNames[i], data);
                } else {
                    String data = field != null ? field.toString() : NULL_VALUE;
                    value.add(data);
                }
            }
            Object data = jsonFormat ? valueMap : value.toString();
            batch.add(data);

        } else if (row instanceof String) {
            batch.add(row);
        } else {
            throw new RuntimeException("The type of element should be 'RowData' or 'String' only.");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            try {
                flush();
            } catch (Exception e) {
                LOGGER.warn("Writing records to doris failed.", e);
                throw new RuntimeException("Writing records to doris failed.", e);
            }
        }
        checkFlushException();
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batch.isEmpty()) {
            return;
        }
        String result;
        if (jsonFormat) {
            if (batch.get(0) instanceof String) {
                result = batch.toString();
            } else {
                result = OBJECT_MAPPER.writeValueAsString(batch);
            }
        } else {
            result = String.join(this.lineDelimiter, batch);
        }
        for (int i = 0; i <= maxRetries; i++) {
            try {
                dorisStreamLoad.load(result);
                batch.clear();
                break;
            } catch (Exception e) {
                LOGGER.error("doris sink error, retry times = {}", i, e);
                if (i >= maxRetries) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(DEFAULT_INTERVAL_MS * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }
}
