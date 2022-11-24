/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.catalog.exception;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

/** Exception for trying to operate on a table that doesn't exist. */
public class TableNotExistException extends SeaTunnelRuntimeException {

    private static final String MSG = "Table %s does not exist in Catalog %s.";

    public TableNotExistException(String catalogName, TablePath tablePath) {
        this(catalogName, tablePath, null);
    }

    public TableNotExistException(String catalogName, TablePath tablePath, Throwable cause) {
        super(SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, String.format(MSG, tablePath.getFullName(), catalogName), cause);
    }
}
