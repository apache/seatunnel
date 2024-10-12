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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaimonRecord {
    public RowKind rowKind;
    public Long pkId;
    public String name;
    public Integer score;
    public String op;
    public String dt;
    public Timestamp oneTime;
    public Timestamp twoTime;
    public Timestamp threeTime;
    public Timestamp fourTime;
    public Integer oneDate;

    public PaimonRecord(Long pkId, String name) {
        this.pkId = pkId;
        this.name = name;
    }

    public PaimonRecord(RowKind rowKind, Long pkId, String name) {
        this(pkId, name);
        this.rowKind = rowKind;
        this.name = name;
    }

    public PaimonRecord(Long pkId, String name, String dt) {
        this(pkId, name);
        this.dt = dt;
    }

    public PaimonRecord(Long pkId, String name, Integer oneDate) {
        this(pkId, name);
        this.oneDate = oneDate;
    }

    public PaimonRecord(
            Long pkId,
            String name,
            Timestamp oneTime,
            Timestamp twoTime,
            Timestamp threeTime,
            Timestamp fourTime) {
        this(pkId, name);
        this.oneTime = oneTime;
        this.twoTime = twoTime;
        this.threeTime = threeTime;
        this.fourTime = fourTime;
    }

    public String toChangeLogFull() {
        Object[] objects = new Object[4];
        objects[0] = rowKind.shortString();
        objects[1] = pkId;
        objects[2] = name;
        objects[3] = score;
        return Arrays.toString(objects);
    }

    public String toChangeLogLookUp() {
        Object[] objects = new Object[5];
        objects[0] = rowKind.shortString();
        objects[1] = pkId;
        objects[2] = name;
        objects[3] = score;
        objects[4] = op;
        return Arrays.toString(objects);
    }
}
