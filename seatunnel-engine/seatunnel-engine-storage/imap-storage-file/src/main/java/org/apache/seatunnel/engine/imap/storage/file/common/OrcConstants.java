/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.orc.TypeDescription;

public class OrcConstants {

    /**
     * orc file schema
     */
    public static final TypeDescription DATA_SCHEMA = TypeDescription.fromString(
        "struct<deleted:boolean,key:binary,keyClass:string,value:binary,valueClass:string,timestamp:bigint>");

    public static final String ORC_FILE_SUFFIX = ".orc";

    /**
     * The path of the archive file
     */
    public static final String ORC_FILE_ARCHIVE_PATH = "archive";

    /**
     * @see org.apache.seatunnel.engine.imap.storage.file.bean.IMapData
     */
    public interface OrcFields{
        String DELETED = "deleted";
        int DELETED_INDEX = 0;
        String KEY = "key";
        int KEY_INDEX = 1;
        String KEY_CLASS = "keyClass";
        int KEY_CLASS_INDEX = 2;
        String VALUE = "value";
        int VALUE_INDEX = 3;
        String VALUE_CLASS = "valueClass";
        int VALUE_CLASS_INDEX = 4;
        String TIMESTAMP = "timestamp";
        int TIMESTAMP_INDEX = 5;
    }
}
