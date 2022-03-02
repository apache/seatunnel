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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;

public class JsonRowOutputFormat extends FileOutputFormat<Row> {

    private static final long serialVersionUID = 1L;

    private static final int NEWLINE = '\n';

    private String charsetName;

    private transient Charset charset;

    private RowTypeInfo rowTypeInfo;

    public JsonRowOutputFormat(Path outputPath, RowTypeInfo rowTypeInfo) {
        this(outputPath, rowTypeInfo, "UTF-8");
    }

    public JsonRowOutputFormat(Path outputPath, RowTypeInfo rowTypeInfo, String charset) {
        super(outputPath);
        this.rowTypeInfo = rowTypeInfo;
        this.charsetName = charset;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) throws IllegalCharsetNameException, UnsupportedCharsetException {
        if (charsetName == null) {
            throw new NullPointerException();
        }

        if (!Charset.isSupported(charsetName)) {
            throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
        }

        this.charsetName = charsetName;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);

        try {
            this.charset = Charset.forName(charsetName);
        } catch (IllegalCharsetNameException e) {
            throw new IOException("The charset " + charsetName + " is not valid.", e);
        } catch (UnsupportedCharsetException e) {
            throw new IOException("The charset " + charsetName + " is not supported.", e);
        }
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        final JSONObject json = getJson(record, rowTypeInfo);
        byte[] bytes = json.toString().getBytes(charset);
        this.stream.write(bytes);
        this.stream.write(NEWLINE);
    }

    private JSONObject getJson(Row record, RowTypeInfo rowTypeInfo) {
        String[] fieldNames = rowTypeInfo.getFieldNames();
        int i = 0;
        JSONObject json = new JSONObject();
        for (String name : fieldNames) {
            Object field = record.getField(i);
            final TypeInformation type = rowTypeInfo.getTypeAt(i);
            if (type instanceof AtomicType) {
                json.put(name, field);
            } else if (type instanceof ObjectArrayTypeInfo) {
                ObjectArrayTypeInfo arrayTypeInfo = (ObjectArrayTypeInfo) type;
                TypeInformation componentInfo = arrayTypeInfo.getComponentInfo();
                JSONArray jsonArray = new JSONArray();
                if (componentInfo instanceof RowTypeInfo) {
                    final Row[] rows = (Row[]) field;
                    for (Row r : rows) {
                        jsonArray.add(getJson(r, (RowTypeInfo) componentInfo));
                    }
                } else {
                    jsonArray.addAll(Arrays.asList((Object[]) field));
                }
                json.put(name, jsonArray);
            } else if (type instanceof RowTypeInfo) {
                RowTypeInfo typeInfo = (RowTypeInfo) type;
                json.put(name, getJson((Row) field, typeInfo));
            }
            i++;
        }
        return json;
    }
}
