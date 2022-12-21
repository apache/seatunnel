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

package io.debezium.connector.dameng.logminer.parser;

import lombok.Getter;

public class SQLReader {
    private final String data;
    @Getter
    private int length;
    @Getter
    private int position;
    @Getter
    private char current;

    public SQLReader(String data) {
        this.data = data;
        this.position = 0;
        this.length = data.length();
        this.current = data.charAt(this.position);
    }

    public boolean current(char c) {
        return this.current == c;
    }

    public void currentCheck(char c, String errorMsg) {
        if (!this.current(c)) {
            throw this.exception(errorMsg);
        }
    }

    public boolean hasNext() {
        return this.position + 1 < this.length;
    }

    public char next() {
        ++this.position;
        this.current = this.data.charAt(this.position);
        return this.current;
    }

    public boolean next(char c) {
        return c == this.next();
    }

    public void moveTo(int pos) {
        this.current = this.data.charAt(pos);
        this.position = pos;
    }

    public boolean skip(int size) {
        this.position += size;
        if (this.position < this.length) {
            this.current = this.data.charAt(this.position);
            return true;
        } else {
            return false;
        }
    }

    public boolean nextAndSkip(SQLSymbolChecker fn) {
        while (true) {
            if (this.hasNext()) {
                if (fn.contains(this.next())) {
                    continue;
                }
                return true;
            }
            return false;
        }
    }

    public boolean equalsIgnoreCaseAndMove(String str) {
        int endPosition = this.position + str.length();
        if (endPosition < this.length
            && str.equalsIgnoreCase(this.data.substring(this.position, endPosition))) {
            this.moveTo(endPosition);
            return true;
        } else {
            return false;
        }
    }

    public boolean equalsAndMove(String str) {
        int endPosition = this.position + str.length();
        if (endPosition < this.length
            && str.equals(this.data.substring(this.position, endPosition))) {
            this.moveTo(endPosition);
            return true;
        } else {
            return false;
        }
    }

    public String loadIn(SQLSymbolChecker checker, String errorMsg) {
        int begin = this.position;

        while (checker.contains(this.current)) {
            this.next();
        }
        if (begin < this.position) {
            String tmp;
            try {
                tmp = this.substring(begin, this.position);
            } finally {
                this.moveTo(this.position - 1);
            }
            return tmp;
        } else {
            throw this.exception(errorMsg);
        }
    }

    public String loadNotIn(SQLSymbolChecker fn) {
        int begin = this.position;
        while (!fn.contains(this.current)) {
            this.next();
        }
        return this.substring(begin, this.position);
    }

    public String loadInQuote(int capacity, char escape) {
        char quote = this.current;
        StringBuilder sb;
        for (sb = new StringBuilder(capacity); !this.next(quote); sb.append(this.current)) {
            if (this.current(escape)) {
                this.next();
            }
        }
        return sb.toString();
    }

    public String loadInQuote(int capacity) {
        char quote = this.current;
        StringBuilder sb = new StringBuilder(capacity);

        while (true) {
            while (!this.next(quote)) {
                sb.append(this.current);
            }
            if (!this.hasNext()) {
                break;
            }
            if (!this.next(quote)) {
                this.moveTo(this.position - 1);
                break;
            }
            sb.append(this.current);
        }
        return sb.toString();
    }

    public String loadInQuoteMulti(int capacity, char endQuote) {
        int layer = 0;
        char quote = this.current;
        StringBuilder sb = new StringBuilder(capacity);
        sb.append(this.current);

        while (true) {
            if (this.next(endQuote)) {
                if (layer <= 0) {
                    sb.append(this.current);
                    return sb.toString();
                }
                --layer;
            } else if (this.current(quote)) {
                ++layer;
            }
            sb.append(this.current);
        }
    }

    public String substring(int begin, int end) {
        return this.data.substring(begin, end);
    }

    public RuntimeException exception(String message) {
        return new RuntimeException(message + ", position " + this.position + ": " + this.data);
    }
}
