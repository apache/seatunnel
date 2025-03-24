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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.system.FtpConnectionMode;

import static org.apache.seatunnel.connectors.seatunnel.file.ftp.system.FtpConnectionMode.ACTIVE_LOCAL;

public class FtpConfigOptions extends BaseSourceConfigOptions {
    public static final Option<String> FTP_PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("FTP server password");
    public static final Option<String> FTP_USERNAME =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("FTP server username");
    public static final Option<String> FTP_HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("FTP server host");
    public static final Option<Integer> FTP_PORT =
            Options.key("port").intType().noDefaultValue().withDescription("FTP server port");
    public static final Option<FtpConnectionMode> FTP_CONNECTION_MODE =
            Options.key("connection_mode")
                    .enumType(FtpConnectionMode.class)
                    .defaultValue(ACTIVE_LOCAL)
                    .withDescription("FTP server connection mode ");
}
