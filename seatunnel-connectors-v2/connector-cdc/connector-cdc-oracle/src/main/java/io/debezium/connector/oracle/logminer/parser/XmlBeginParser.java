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

package io.debezium.connector.oracle.logminer.parser;

import io.debezium.annotation.NotThreadSafe;

/**
 * Simple text-based parser implementation for Oracle LogMiner XML_BEGIN Redo SQL.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class XmlBeginParser extends PreambleSingleColumnReconstructedSelectParser {

    private static final String PREAMBLE = "XML DOC BEGIN:";

    public XmlBeginParser() {
        super(PREAMBLE);
    }

    @Override
    protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
        return LogMinerDmlEntryImpl.forXml(columnValues);
    }
}
