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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.email.config.EmailSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.email.sink.EmailSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class EmailFactoryTest {

    private final OptionRule optionRule = new EmailSinkFactory().optionRule();

    @Test
    void testValidSmtpPorts() {
        Map<String, Object> config = requiredConfig();
        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), 1);
        Assertions.assertDoesNotThrow(() -> validate(config));

        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), 465);
        Assertions.assertDoesNotThrow(() -> validate(config));

        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), 65535);
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void testDefaultOptionalOptions() {
        Assertions.assertDoesNotThrow(() -> validate(requiredConfig()));
    }

    @Test
    void testExplicitOptionalOptions() {
        Map<String, Object> config = requiredConfig();
        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), 465);
        config.put(EmailSinkOptions.EMAIL_ATTACHMENT_NAME.key(), "report.csv");
        config.put(EmailSinkOptions.EMAIL_FIELD_DELIMITER.key(), "|");
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void testInvalidSmtpPorts() {
        Map<String, Object> config = requiredConfig();
        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        config.put(EmailSinkOptions.EMAIL_SMTP_PORT.key(), 65536);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "EmailSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(EmailSinkOptions.EMAIL_FROM_ADDRESS.key(), "sender@example.com");
        config.put(EmailSinkOptions.EMAIL_TO_ADDRESS.key(), "receiver@example.com");
        config.put(EmailSinkOptions.EMAIL_HOST.key(), "smtp.example.com");
        config.put(EmailSinkOptions.EMAIL_TRANSPORT_PROTOCOL.key(), "smtp");
        config.put(EmailSinkOptions.EMAIL_SMTP_AUTH.key(), true);
        config.put(EmailSinkOptions.EMAIL_AUTHORIZATION_CODE.key(), "code");
        config.put(EmailSinkOptions.EMAIL_MESSAGE_HEADLINE.key(), "subject");
        config.put(EmailSinkOptions.EMAIL_MESSAGE_CONTENT.key(), "content");
        return config;
    }
}
