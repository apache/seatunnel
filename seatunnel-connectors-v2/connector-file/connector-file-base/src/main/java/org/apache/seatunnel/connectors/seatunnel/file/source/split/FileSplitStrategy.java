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
package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import java.io.Serializable;
import java.util.List;

/**
 * {@link FileSplitStrategy} defines the contract for splitting a file into one or more {@link
 * FileSourceSplit}s that can be processed in parallel by file-based sources.
 *
 * <p>The split strategy determines how a file is logically divided, such as by byte ranges, record
 * boundaries, or format-specific physical units. Implementations are responsible for ensuring that
 * each generated split is readable and does not violate the semantics of the underlying file
 * format.
 *
 * <p>The resulting {@link FileSourceSplit}s describe the portion of the file to be read, while the
 * actual data parsing and decoding are handled by the corresponding reader implementation.
 */
public interface FileSplitStrategy extends Serializable {

    List<FileSourceSplit> split(String tableId, String filePath);
}
