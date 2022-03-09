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

package org.apache.seatunnel.flink.source;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class TextRowInputFormat extends DelimitedInputFormat<Row> implements ResultTypeQueryable<Row> {

    private static final long serialVersionUID = -7881695870867510222L;

    private static final byte CARRIAGE_RETURN = (byte) '\r';

    private static final byte NEW_LINE = (byte) '\n';

    private String charsetName = "UTF-8";

    public TextRowInputFormat(Path filePath) {
        super(filePath, null);
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        if (charsetName == null) {
            throw new IllegalArgumentException("Charset must not be null.");
        }

        this.charsetName = charsetName;
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (charsetName == null || !Charset.isSupported(charsetName)) {
            throw new RuntimeException("Unsupported charset: " + charsetName);
        }
    }

    @Override
    public Row readRecord(Row reusable, byte[] bytes, int offset, int numBytes) throws IOException {
        if (this.getDelimiter() != null
                && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE
                && offset + numBytes >= 1
                && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
            numBytes -= 1;
        }
        String str = new String(bytes, offset, numBytes, this.charsetName);
        reusable.setField(0, str);
        return reusable;
    }

    @Override
    public String toString() {
        return "TextRowInputFormat (" + Arrays.toString(getFilePaths()) + ") - " + this.charsetName;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        TypeInformation[] info = {Types.STRING()};
        String[] name = {"message"};
        return new RowTypeInfo(info, name);
    }
}
