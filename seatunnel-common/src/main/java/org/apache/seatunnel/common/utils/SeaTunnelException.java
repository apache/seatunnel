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

public class SeaTunnelException extends RuntimeException {

    /**
     * Required for serialization support.
     *
     * @see java.io.Serializable
     */
    private static final long serialVersionUID = 2263144814025689516L;

    /**
     * <p>Constructs a new {@code SeaTunnelException} without specified
     * detail message.</p>
     */
    public SeaTunnelException() {
    }

    /**
     * <p>Constructs a new {@code SeaTunnelException} with specified
     * detail message.</p>
     *
     * @param msg The error message.
     */
    public SeaTunnelException(final String msg) {
        super(msg);
    }

    /**
     * <p>Constructs a new {@code SeaTunnelException} with specified
     * nested {@code Throwable}.</p>
     *
     * @param cause The {@code Exception} or {@code Error}
     *              that caused this exception to be thrown.
     */
    public SeaTunnelException(final Throwable cause) {
        super(cause);
    }

    /**
     * <p>Constructs a new {@code SeaTunnelException} with specified
     * detail message and nested {@code Throwable}.</p>
     *
     * @param msg   The error message.
     * @param cause The {@code Exception} or {@code Error}
     *              that caused this exception to be thrown.
     */
    public SeaTunnelException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
