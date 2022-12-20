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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileCopyMethod;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;

public class FileTransferFactory {
    public static FileTransfer createFileTransfer(ClickhouseFileCopyMethod type, String host, String user, String password) {
        switch (type) {
            case SCP:
                return new ScpFileTransfer(host, user, password);
            case RSYNC:
                return new RsyncFileTransfer(host, user, password);
            default:
                throw new ClickhouseConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, "unsupported clickhouse file copy method:" + type);
        }
    }
}
