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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.transform.encrypt.encryptor.AesGcmEncryptor;

import java.util.List;

public class FieldEncryptTransformConfig {
    public static final Option<List<String>> FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The list of fields that need to be encrypted.");

    public static final Option<String> ALGORITHM =
            Options.key("algorithm")
                    .stringType()
                    .defaultValue(AesGcmEncryptor.IDENTIFIER)
                    .withDescription(
                            "The encryption algorithm, Supported values: AES_CBC (default), AES_GCM");

    public static final Option<String> KEY =
            Options.key("key").stringType().noDefaultValue().withDescription("The encryption key.");

    public static final Option<String> MODE =
            Options.key("mode")
                    .stringType()
                    .defaultValue("encrypt")
                    .withDescription("The mode of the transform, support encrypt and decrypt.");

    public static final Option<Integer> MAX_FIELD_LENGTH =
            Options.key("max_field_length")
                    .intType()
                    .defaultValue(10 * 1024 * 1024) // 10MB
                    .withDescription("Maximum field length to encrypt");
}
