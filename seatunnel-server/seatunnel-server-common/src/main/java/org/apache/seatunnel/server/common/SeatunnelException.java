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

package org.apache.seatunnel.server.common;

public class SeatunnelException extends RuntimeException{
    private SeatunnelErrorEnum errorEnum;

    public SeatunnelException(SeatunnelErrorEnum e) {
        super(e.getMsg());
        this.errorEnum = e;
    }

    public SeatunnelException(SeatunnelErrorEnum e, Object... msg) {
        super(String.format(e.getTemplate(), msg));
        this.errorEnum = e;
    }

    public static SeatunnelException newInstance(SeatunnelErrorEnum e, Object... msg) {
        return new SeatunnelException(e, msg);

    }

    public static SeatunnelException newInstance(SeatunnelErrorEnum e) {
        return new SeatunnelException(e);

    }

    public SeatunnelErrorEnum getErrorEnum() {
        return errorEnum;
    }
}
