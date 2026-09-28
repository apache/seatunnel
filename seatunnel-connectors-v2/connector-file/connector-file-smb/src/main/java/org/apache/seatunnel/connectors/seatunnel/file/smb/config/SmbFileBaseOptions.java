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

package org.apache.seatunnel.connectors.seatunnel.file.smb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;

public class SmbFileBaseOptions extends FileBaseOptions {
    public static final Option<String> SMB_HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("SMB server host");
    public static final Option<Integer> SMB_PORT =
            Options.key("port").intType().defaultValue(445).withDescription("SMB server port");
    public static final Option<String> SMB_USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SMB server username");
    public static final Option<String> SMB_PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SMB server password");
    public static final Option<String> SMB_DOMAIN =
            Options.key("domain")
                    .stringType()
                    .defaultValue("")
                    .withDescription("SMB authentication domain");
    public static final Option<String> SMB_SHARE =
            Options.key("share").stringType().noDefaultValue().withDescription("SMB share name");
}
