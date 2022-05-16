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

package org.apache.seatunnel.common.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class SerializationUtils {

    public static String objectToString(Serializable obj) {
        if (obj != null) {
            return Base64.encodeBase64String(SerializationUtils.serialize(obj));
        }
        return null;
    }

    public static <T extends Serializable> T stringToObject(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return org.apache.commons.lang3.SerializationUtils.deserialize(Base64.decodeBase64(str));
        }
        return null;
    }

    public static <T extends Serializable> byte[] serialize(T obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize(obj);
    }

    public static <T extends Serializable> T deserialize(byte[] bytes) {
        return org.apache.commons.lang3.SerializationUtils.deserialize(bytes);
    }

}
