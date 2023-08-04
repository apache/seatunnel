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

package org.apache.seatunnel.connectors.seatunnel.file.sink.commit;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

@Data
@AllArgsConstructor
public class FileCommitInfo implements Serializable {
    /**
     * Storage the commit info in map.
     *
     * <p>K is the file path need to be moved to target dir.
     *
     * <p>V is the target file path of the data file.
     */
    private final LinkedHashMap<String, String> needMoveFiles;

    /**
     * Storage the partition information in map.
     *
     * <p>K is the partition column's name.
     *
     * <p>V is the list of partition column's values.
     */
    private final LinkedHashMap<String, List<String>> partitionDirAndValuesMap;

    /** Storage the transaction directory */
    private final String transactionDir;
}
