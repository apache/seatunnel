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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class FakeOptions implements Serializable {

    private static final String ROW_NUM = "row.num";
    private static final Long DEFAULT_ROW_NUM = 10L;
    @Getter
    @Setter
    private Long rowNum;

    public static FakeOptions parse(Config config) {
        FakeOptions fakeOptions = new FakeOptions();
        fakeOptions.setRowNum(config.hasPath(ROW_NUM) ? config.getLong(ROW_NUM) : DEFAULT_ROW_NUM);
        return fakeOptions;
    }
}
