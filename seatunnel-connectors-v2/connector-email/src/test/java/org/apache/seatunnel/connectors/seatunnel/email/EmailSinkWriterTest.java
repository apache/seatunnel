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

package org.apache.seatunnel.connectors.seatunnel.email;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.email.config.EmailSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.email.sink.EmailSinkWriter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class EmailSinkWriterTest {

    @Test
    void testWriteWithNullValues() {
        // Create a mock config
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("email_from_address", "test@example.com");
        configMap.put("email_to_address", "receiver@example.com");
        configMap.put("email_authorization_code", "code");
        configMap.put("email_message_headline", "Test");
        configMap.put("email_message_content", "Test content");
        configMap.put("email_host", "smtp.example.com");
        configMap.put("email_transport_protocol", "smtp");
        configMap.put("email_smtp_auth", true);
        configMap.put("email_smtp_port", 465);
        configMap.put("email_attachment_name", "test.csv");
        configMap.put("email_field_delimiter", ",");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        EmailSinkConfig sinkConfig = new EmailSinkConfig(config);

        // Create row type with string fields
        String[] fieldNames = {"field1", "field2", "field3"};
        SeaTunnelDataType<?>[] fieldTypes = {
            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
        };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);

        // Create writer
        EmailSinkWriter writer = new EmailSinkWriter(rowType, sinkConfig);

        // Test writing row with null values - should not throw NPE
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"value1", null, "value3"});

        Assertions.assertDoesNotThrow(() -> writer.write(row));

        // Test writing row with all null values - should not throw NPE
        SeaTunnelRow nullRow = new SeaTunnelRow(new Object[] {null, null, null});

        Assertions.assertDoesNotThrow(() -> writer.write(nullRow));
    }

    @Test
    void testCustomDelimiter() {
        // Create a mock config with custom delimiter
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("email_from_address", "test@example.com");
        configMap.put("email_to_address", "receiver@example.com");
        configMap.put("email_authorization_code", "code");
        configMap.put("email_message_headline", "Test");
        configMap.put("email_message_content", "Test content");
        configMap.put("email_host", "smtp.example.com");
        configMap.put("email_transport_protocol", "smtp");
        configMap.put("email_smtp_auth", true);
        configMap.put("email_smtp_port", 465);
        configMap.put("email_attachment_name", "test.csv");
        configMap.put("email_field_delimiter", "|");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        EmailSinkConfig sinkConfig = new EmailSinkConfig(config);

        Assertions.assertEquals("|", sinkConfig.getEmailFieldDelimiter());
        Assertions.assertEquals("test.csv", sinkConfig.getEmailAttachmentName());
    }
}
