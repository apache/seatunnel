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

package org.apache.seatunnel.common.exception;

/** SeaTunnel global exception, used to tell user more clearly error messages */
public class SeaTunnelRuntimeException extends RuntimeException {
    private final SeaTunnelErrorCode seaTunnelErrorCode;

    public SeaTunnelRuntimeException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode.getErrorMessage() + " - " + errorMessage);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
    }

    public SeaTunnelRuntimeException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode.getErrorMessage() + " - " + errorMessage, cause);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
    }

    public SeaTunnelRuntimeException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode.getErrorMessage(), cause);
        this.seaTunnelErrorCode = seaTunnelErrorCode;
    }
}
