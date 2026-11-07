/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.mailchimp;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.constants.JobMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MailchimpConfigTest {
    static Map<String, Object> options() {
        return new HashMap<>(
                ConfigFactory.parseString(
                                "token=testkey-us1\nlist_id=audience1\nschema { fields { "
                                        + "id=string, email_address=string, status=string, last_changed=string, "
                                        + "merge_fields=\"map<string,string>\" } }")
                        .root()
                        .unwrapped());
    }

    static ReadonlyConfig config(Map<String, Object> options) {
        return ReadonlyConfig.fromMap(options);
    }

    @Test
    void defaultsAndBoundedMode() throws Exception {
        MailchimpConfig cfg = new MailchimpConfig(config(options()));
        Assertions.assertEquals(
                "https://us1.api.mailchimp.com/3.0/lists/audience1/members",
                cfg.membersUri.toString());
        Assertions.assertEquals(1000, cfg.pageSize);
        MailchimpSource source = new MailchimpSource(config(options()));
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        Assertions.assertEquals(
                5, source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getTotalFields());
        JobContext streaming = new JobContext();
        streaming.setJobMode(JobMode.STREAMING);
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> source.setJobContext(streaming));
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> source.createReader(context));
    }

    @ParameterizedTest
    @ValueSource(strings = {"token", "list_id", "schema"})
    void missingRequired(String key) {
        Map<String, Object> opts = options();
        opts.remove(key);
        Assertions.assertThrows(Exception.class, () -> new MailchimpSource(config(opts)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "secret", "secret-us1\r\nInjected", "secret-us1.evil"})
    void invalidKeysAreNotEchoed(String value) {
        Map<String, Object> opts = options();
        opts.put("token", value);
        Exception error =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new MailchimpConfig(config(opts)));
        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains("secret"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "../other", "x?fields=email_address", "x/y", "x%2fy", "x#fragment"})
    void listCannotChangeRequestTarget(String value) {
        Map<String, Object> opts = options();
        opts.put("list_id", value);
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new MailchimpConfig(config(opts)));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "https://secret@host/3.0",
                "https://host/3.0?secret=1",
                "https://host/3.0#secret",
                "file:///3.0",
                "https://host/3.0/evil",
                "https://host:0/3.0",
                "https://host:65536/3.0",
                "https://host/%33.0",
                "https://host/3.0\\evil",
                "not a secret uri"
            })
    void rejectsUnsafeUriShapesWithoutEcho(String url) {
        Map<String, Object> opts = options();
        opts.put("base_url", url);
        Exception error =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new MailchimpConfig(config(opts)));
        Assertions.assertFalse(error.getMessage().contains("secret"));
        Assertions.assertNull(error.getCause());
    }

    @Test
    void explicitEndpointAndNumericBounds() {
        Map<String, Object> opts = options();
        opts.put("base_url", "https://trusted.example:8443/3.0/");
        opts.put("page_size", 1);
        opts.put("retry", 0);
        Assertions.assertEquals(
                "https://trusted.example:8443/3.0/lists/audience1/members",
                new MailchimpConfig(config(opts)).membersUri.toString());
        Map<String, int[]> bad = new HashMap<>();
        bad.put("page_size", new int[] {0, 1001});
        bad.put("retry", new int[] {-1, 11});
        bad.put("retry_backoff_ms", new int[] {0, 60001});
        bad.put("request_timeout_ms", new int[] {0, 300001});
        bad.forEach(
                (key, values) -> {
                    for (int value : values) {
                        Map<String, Object> invalid = options();
                        invalid.put(key, value);
                        Assertions.assertThrows(
                                IllegalArgumentException.class,
                                () -> new MailchimpConfig(config(invalid)));
                    }
                });
        opts.put("schema", new HashMap<>());
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new MailchimpSource(config(opts)));
    }

    @Test
    void httpRequiresExplicitFixtureOptIn() {
        Map<String, Object> opts = options();
        opts.put("base_url", "http://localhost:8080/3.0");
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new MailchimpConfig(config(opts)));
        opts.put("allow_insecure_http", true);
        Assertions.assertEquals("http", new MailchimpConfig(config(opts)).membersUri.getScheme());
    }
}
