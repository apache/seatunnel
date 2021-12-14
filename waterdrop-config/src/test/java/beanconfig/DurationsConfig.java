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
package beanconfig;

import java.time.Duration;

public class DurationsConfig {
    Duration second;
    Duration secondAsNumber;
    Duration halfSecond;


    public Duration getSecond() {
        return second;
    }

    public void setSecond(Duration second) {
        this.second = second;
    }

    public Duration getSecondAsNumber() {
        return secondAsNumber;
    }

    public void setSecondAsNumber(Duration secondAsNumber) {
        this.secondAsNumber = secondAsNumber;
    }

    public Duration getHalfSecond() {
        return halfSecond;
    }

    public void setHalfSecond(Duration halfSecond) {
        this.halfSecond = halfSecond;
    }
}
