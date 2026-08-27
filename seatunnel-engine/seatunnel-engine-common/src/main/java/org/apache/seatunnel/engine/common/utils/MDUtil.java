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

package org.apache.seatunnel.engine.common.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MDUtil {
    /** Algorithm to be used for message digest. */
    private static final String HASHING_ALGORITHM = "SHA-1";

    /**
     * Creates a new instance of the message digest.
     *
     * @return a new instance of the message digest
     */
    public static MessageDigest createMessageDigest() {
        try {
            return MessageDigest.getInstance(HASHING_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(
                    "Cannot instantiate the message digest algorithm " + HASHING_ALGORITHM, e);
        }
    }
}
