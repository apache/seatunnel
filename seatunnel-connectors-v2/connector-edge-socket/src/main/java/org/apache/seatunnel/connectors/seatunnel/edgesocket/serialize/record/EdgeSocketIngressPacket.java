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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record;

import lombok.Data;

@Data
public class EdgeSocketIngressPacket {

    /** Protocol version for future packet format evolution. */
    private Integer version;

    /** Base64 payload body; sender should apply compression/encryption before encoding. */
    private String payload;

    /** Compression type of payload bytes (none/gzip/zlib/deflate). */
    private String compression;

    /** Encryption type of payload bytes (none/aes_gcm). */
    private String encryption;

    /** Base64 IV used by AES_GCM, required when encryption is aes_gcm. */
    private String iv;
}
