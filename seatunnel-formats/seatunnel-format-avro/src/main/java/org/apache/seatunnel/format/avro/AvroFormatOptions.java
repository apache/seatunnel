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

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options that control how the Avro format serializes and deserializes SeaTunnel rows. */
public class AvroFormatOptions {

    public static final Option<Boolean> AVRO_ROW_NULLABLE =
            Options.key("avro_row_nullable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether a ROW field, including ROW values nested in ARRAY and MAP, is "
                                    + "written as a nullable Avro union of [null, record] instead of "
                                    + "a bare Avro record. Enabling it lets a null ROW value be "
                                    + "serialized. The option is disabled by default because it "
                                    + "changes the binary layout of every schema that contains a ROW "
                                    + "field, so the writer and the reader must be enabled and "
                                    + "disabled together; messages written with the default layout "
                                    + "cannot be read with the option enabled unless the original "
                                    + "writer schema is supplied explicitly.");
}
