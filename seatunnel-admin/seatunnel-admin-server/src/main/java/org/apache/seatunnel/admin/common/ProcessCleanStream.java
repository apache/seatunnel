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
package org.apache.seatunnel.admin.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ProcessCleanStream extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ProcessCleanStream.class);

    InputStream inputStream;
    String processName;
    String type;
    StringBuilder logs = new StringBuilder();
    private Boolean isEnd = false;

    public String getLogs() throws InterruptedException {
        int total = 0;
        while (!isEnd) {
            Thread.sleep(50);
            total += 50;
            if (total > 10000) {
                break;
            }
        }
        return logs.toString();
    }

    public ProcessCleanStream(InputStream stream, String processName, String type) {
        this.inputStream = stream;
        this.processName = processName;
        this.type = type;
    }

    @Override
    public void run() {
        InputStreamReader isr = null;
        try {
            isr = new InputStreamReader(inputStream, "utf-8");
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                if (logs.length() < 256 * 1024) {
                    logs.append(line).append("\n");
                } else {
                    logs.append(line.substring(0, 255 * 1024)).append("\n");
                }
                if (type.equalsIgnoreCase("error")) {
                    logger.warn("[" + processName + "]" + line);
                } else if (type.equalsIgnoreCase("info")) {
                    logger.info("[" + processName + "]" + line);
                }
            }
        } catch (IOException ioe) {
            logger.error("日志流异常", ioe);
        } finally {
            isEnd = true;
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
