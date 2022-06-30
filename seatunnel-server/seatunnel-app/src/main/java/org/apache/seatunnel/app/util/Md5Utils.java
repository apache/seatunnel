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

package org.apache.seatunnel.app.util;

import java.security.MessageDigest;

public class Md5Utils {
    private static final String[] HEX_DIGITS = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

    public Md5Utils() {
    }

    public static String byteArrayToHexString(byte[] b) {
        StringBuilder resultSb = new StringBuilder();
        byte[] var2 = b;
        int var3 = b.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            byte aB = var2[var4];
            resultSb.append(byteToHexString(aB));
        }

        return resultSb.toString();
    }

    @SuppressWarnings("magicnumber")
    private static String byteToHexString(byte b) {
        int n = b;
        if (b < 0) {
            n = 256 + b;
        }

        int d1 = n / 16;
        int d2 = n % 16;
        return HEX_DIGITS[d1] + HEX_DIGITS[d2];
    }

    public static String md5Encode(String origin) {
        String resultString = null;
        String charsetname = "UTF-8";

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            resultString = byteArrayToHexString(md.digest(origin.getBytes(charsetname)));
        } catch (Exception var4) {
            var4.printStackTrace();
        }

        return resultString;
    }
}
