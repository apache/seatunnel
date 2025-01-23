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

package org.apache.seatunnel.format.csv.constant;

import java.io.Serializable;

/** @see org.apache.commons.csv.QuoteMode */
public enum CsvStringQuoteMode implements Serializable {
    /** Quotes all fields. */
    ALL,

    /**
     * Quotes fields which contain special characters such as a the field delimiter, quote character
     * or any of the characters in the line separator string.
     */
    MINIMAL,

    /**
     * Never quotes fields. When the delimiter occurs in data, the printer prefixes it with the
     * escape character. If the escape character is not set, format validation throws an exception.
     */
    NONE
}
