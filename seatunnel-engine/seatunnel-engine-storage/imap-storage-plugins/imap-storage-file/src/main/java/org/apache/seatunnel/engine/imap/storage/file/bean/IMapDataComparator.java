/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.bean;

final class IMapDataComparator {

    private IMapDataComparator() {}

    static int compare(
            boolean leftDeleted,
            byte[] leftKey,
            String leftKeyClassName,
            byte[] leftValue,
            String leftValueClassName,
            long leftTimestamp,
            boolean rightDeleted,
            byte[] rightKey,
            String rightKeyClassName,
            byte[] rightValue,
            String rightValueClassName,
            long rightTimestamp) {
        int keyClassCompare = compareNullableString(leftKeyClassName, rightKeyClassName);
        if (keyClassCompare != 0) {
            return keyClassCompare;
        }

        int keyCompare = compareBytes(leftKey, rightKey);
        if (keyCompare != 0) {
            return keyCompare;
        }

        int timestampCompare = Long.compare(rightTimestamp, leftTimestamp);
        if (timestampCompare != 0) {
            return timestampCompare;
        }

        // WAL timestamps have millisecond precision and Hadoop FileSystem does not provide a
        // portable, atomic global sequence across HDFS, S3, and OSS writers. These remaining
        // comparisons therefore provide a deterministic total order for timestamp collisions;
        // they do not claim general last-write-wins ordering across independently active writers.
        int deleteCompare = Boolean.compare(rightDeleted, leftDeleted);
        if (deleteCompare != 0) {
            return deleteCompare;
        }

        int valueClassCompare = compareNullableString(leftValueClassName, rightValueClassName);
        if (valueClassCompare != 0) {
            return valueClassCompare;
        }

        return compareBytes(leftValue, rightValue);
    }

    private static int compareNullableString(String left, String right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        return left.compareTo(right);
    }

    private static int compareBytes(byte[] left, byte[] right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        int length = Math.min(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int compare = Byte.compare(left[i], right[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
