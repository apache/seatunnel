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

package org.apache.seatunnel.config.beanconfig;

public class NumbersConfig {

    private int intVal;
    private Integer intObj;
    private long longVal;
    private Long longObj;
    private double doubleVal;
    private Double doubleObj;

    public int getIntVal() {
        return intVal;
    }

    public void setIntVal(int intVal) {
        this.intVal = intVal;
    }

    public Integer getIntObj() {
        return intObj;
    }

    public void setIntObj(Integer intObj) {
        this.intObj = intObj;
    }

    public long getLongVal() {
        return longVal;
    }

    public void setLongVal(long longVal) {
        this.longVal = longVal;
    }

    public Long getLongObj() {
        return longObj;
    }

    public void setLongObj(Long longObj) {
        this.longObj = longObj;
    }

    public double getDoubleVal() {
        return doubleVal;
    }

    public void setDoubleVal(double doubleVal) {
        this.doubleVal = doubleVal;
    }

    public Double getDoubleObj() {
        return doubleObj;
    }

    public void setDoubleObj(Double doubleObj) {
        this.doubleObj = doubleObj;
    }
}
