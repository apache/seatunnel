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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.UnsupportedEncodingException;

@Data
@AllArgsConstructor
public class FileRowEvent {
    private long pos;
    private String body;
    private boolean end;
    private String separator;
    private int originalLength;
    private boolean truncated;

    public void mergeMultiline(
            String separator, FileRowEvent event, int maxMessageBytes, String charset)
            throws UnsupportedEncodingException {
        if (!truncated && this.getOriginalLength() < maxMessageBytes) {
            if (this.getOriginalLength() + event.getOriginalLength() <= maxMessageBytes) {
                this.body = this.body + separator + event.body;
                this.originalLength =
                        this.originalLength
                                + event.getOriginalLength()
                                + separator.getBytes().length;
                this.truncated = event.isTruncated();
            } else {
                int remainingBytes = maxMessageBytes - this.originalLength;
                String remainingStr =
                        new String(event.getBody().getBytes(charset), 0, remainingBytes, charset);
                this.body = this.body + separator + remainingStr;
                this.originalLength =
                        this.originalLength
                                + event.getOriginalLength()
                                + separator.getBytes().length;
                this.truncated = true;
            }
        }
        if (event.isEnd() && event.getSeparator() != null) {
            this.separator = event.getSeparator();
        }
        this.end = event.isEnd();
    }
}
