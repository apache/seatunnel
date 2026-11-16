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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.LIST_ID;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.RETRY;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.TOKEN;

final class MailchimpConfig implements Serializable {
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9]+-(us[0-9]+)");
    final String apiKey;
    final URI membersUri;
    final int pageSize;
    final int retry;
    final int backoffMs;
    final int timeoutMs;

    MailchimpConfig(ReadonlyConfig config) {
        ConfigValidator.of(config).validate(new MailchimpSourceFactory().optionRule());
        apiKey = config.get(TOKEN);
        Matcher key = KEY_PATTERN.matcher(apiKey);
        if (!key.matches()) {
            throw new IllegalArgumentException(
                    "Mailchimp token must include a valid datacenter suffix");
        }
        String listId = config.get(LIST_ID);
        if (!listId.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("Mailchimp list_id must be a nonempty identifier");
        }
        String base =
                config.getOptional(BASE_URL)
                        .orElse("https://" + key.group(1) + ".api.mailchimp.com/3.0");
        try {
            URI uri = new URI(base);
            if ("http".equals(uri.getScheme()) && !config.get(ALLOW_INSECURE_HTTP)) {
                throw new IllegalArgumentException(
                        "Mailchimp base_url requires HTTPS; allow_insecure_http is only for isolated fixtures with dummy credentials");
            }
            if ((!"https".equals(uri.getScheme()) && !"http".equals(uri.getScheme()))
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (uri.getPort() != -1 && (uri.getPort() < 1 || uri.getPort() > 65535))
                    || (!"/3.0".equals(uri.getRawPath()) && !"/3.0/".equals(uri.getRawPath()))) {
                throw new IllegalArgumentException(
                        "Mailchimp base_url must be a trusted HTTP(S) API root ending in /3.0, without credentials, query or fragment");
            }
            membersUri =
                    new URI(
                            uri.getScheme(),
                            null,
                            uri.getHost(),
                            uri.getPort(),
                            "/3.0/lists/" + listId + "/members",
                            null,
                            null);
        } catch (URISyntaxException e) {
            // URI parser messages can contain credentials from invalid user input.
            throw new IllegalArgumentException("Mailchimp base_url is not a valid API root");
        }
        pageSize = range(config.get(PAGE_SIZE), 1, 1000, PAGE_SIZE.key());
        retry = range(config.get(RETRY), 0, 10, RETRY.key());
        backoffMs = range(config.get(RETRY_BACKOFF_MS), 1, 60000, RETRY_BACKOFF_MS.key());
        timeoutMs = range(config.get(REQUEST_TIMEOUT_MS), 1, 300000, REQUEST_TIMEOUT_MS.key());
    }

    private static int range(int value, int min, int max, String option) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "Mailchimp " + option + " must be between " + min + " and " + max);
        }
        return value;
    }
}
