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

package org.apache.seatunnel.connectors.seatunnel.file.config;

/**
 * Behavior when a target file already exists during commit (e.g. moving/renaming a temp file to the
 * final path).
 */
public enum FileExistsMode {
    /** Skip writing the file when the target already exists. */
    SKIP,
    /** Overwrite the target file when it already exists. */
    OVERWRITE,
    /** Fail the job when the target file already exists. */
    FAIL
}
