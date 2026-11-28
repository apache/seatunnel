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

package org.apache.seatunnel.connectors.seatunnel.blogger;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.MAX_BLOGS;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSourceOptions.TOKEN;

/**
 * Validates the read-only connection and plans only documented resource paths.
 *
 * <p>The scope binds saved positions to the selected resource, paging configuration and Google
 * principal represented by the configured refresh token.
 */
final class BloggerConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern DECIMAL_ID = Pattern.compile("[0-9]{1,64}");
    static final String API_HOST = "blogger.googleapis.com";
    static final String OAUTH_HOST = "oauth2.googleapis.com";
    static final String READONLY_SCOPE = "https://www.googleapis.com/auth/blogger.readonly";

    final String clientId;
    final String clientSecret;
    final String refreshToken;
    final Resource resource;
    final int pageSize;
    final int maxPages;
    final int maxBlogs;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    BloggerConfig(ReadonlyConfig config) {
        clientId = credential(config.get(CLIENT_ID), "client_id");
        clientSecret = credential(config.get(CLIENT_SECRET), "client_secret");
        refreshToken = credential(config.get(TOKEN), "token");
        resource = Resource.parse(config.get(RESOURCE));
        pageSize = range(config, PAGE_SIZE, 1, 100);
        maxPages = range(config, MAX_PAGES, 1, 10000);
        maxBlogs = range(config, MAX_BLOGS, 1, 50000);
        connectTimeout = range(config, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(config, READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(config, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(config, MAX_ATTEMPTS, 1, 10);
        maxBytes = range(config, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        scope =
                BloggerJson.digest(
                        "blogger/v3|"
                                + resource.id
                                + "|"
                                + pageSize
                                + "|"
                                + encode(clientId)
                                + "|"
                                + encode(refreshToken));
    }

    /**
     * Forms a fixed relative GET request. Discovery reads exclude post bodies.
     *
     * @param stage the active API operation
     * @param blog parent blog ID, where required
     * @param post parent post ID, where required
     * @param token the saved opaque page token
     * @param discovery whether the response only supplies parent identities
     * @return relative request target for the Blogger API
     */
    String target(Stage stage, String blog, String post, String token, boolean discovery) {
        if (stage == Stage.BLOG_LIST) {
            return "/v3/users/self/blogs";
        }
        if (!validId(blog)) {
            throw invalid("invalid discovered blog ID");
        }
        String blogPath = "/v3/blogs/" + blog;
        if ((stage == Stage.POST_DETAIL || stage == Stage.COMMENT_LIST) && !validId(post)) {
            throw invalid("invalid discovered post ID");
        }
        if (stage == Stage.POST_DETAIL) {
            return blogPath + "/posts/" + post + "?fetchBody=true";
        }
        String path;
        if (stage == Stage.POST_LIST) {
            path = blogPath + "/posts";
        } else if (stage == Stage.PAGE_LIST) {
            path = blogPath + "/pages";
        } else {
            path = blogPath + "/posts/" + post + "/comments";
        }
        return path
                + "?maxResults="
                + pageSize
                + "&fetchBodies="
                + !discovery
                + (token == null ? "" : "&pageToken=" + encode(token));
    }

    /**
     * Encodes Google OAuth refresh credentials only for the fixed token host.
     *
     * @return UTF-8 application/x-www-form-urlencoded refresh grant
     */
    byte[] refreshBody() {
        return ("grant_type=refresh_token&client_id="
                        + encode(clientId)
                        + "&client_secret="
                        + encode(clientSecret)
                        + "&refresh_token="
                        + encode(refreshToken))
                .getBytes(StandardCharsets.UTF_8);
    }

    int maximumRows(Stage stage) {
        if (stage == Stage.BLOG_LIST) {
            return maxBlogs;
        }
        return stage == Stage.POST_DETAIL ? 1 : pageSize;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    static boolean validId(String id) {
        return id != null && DECIMAL_ID.matcher(id).matches();
    }

    static boolean validPageToken(String token) {
        return validText(token, 4096);
    }

    private static boolean validText(String value, int limit) {
        return value != null
                && !value.isBlank()
                && value.length() <= limit
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    private static String credential(String value, String option) {
        if (!validText(value, 8192)) {
            throw invalid(option + " must be a nonempty valid OAuth credential");
        }
        return value;
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Blogger configuration: " + message);
    }

    /**
     * Selects the stages required for a provider output.
     *
     * <p>Only the final stage emits rows, even if earlier stages use the same API collection as a
     * different configured resource.
     */
    enum Resource {
        USERS("users", Stage.BLOG_LIST),
        BLOGS("blogs", Stage.BLOG_LIST, Stage.POST_LIST),
        POSTS("posts", Stage.BLOG_LIST, Stage.POST_LIST, Stage.POST_DETAIL),
        PAGES("pages", Stage.BLOG_LIST, Stage.PAGE_LIST),
        COMMENTS("comments", Stage.BLOG_LIST, Stage.POST_LIST, Stage.COMMENT_LIST);

        final String id;
        final List<Stage> stages;

        Resource(String id, Stage... stages) {
            this.id = id;
            this.stages = List.of(stages);
        }

        static Resource parse(String value) {
            for (Resource resource : values()) {
                if (resource.id.equals(value)) {
                    return resource;
                }
            }
            throw invalid("resource must name a supported Blogger collection");
        }
    }

    /**
     * A distinct list or detail response in the documented discovery graph.
     *
     * <p>Two stages have no continuation and never send pageToken.
     */
    enum Stage {
        BLOG_LIST("blogger#blogList", false),
        POST_LIST("blogger#postList", true),
        POST_DETAIL("blogger#post", false),
        PAGE_LIST("blogger#pageList", true),
        COMMENT_LIST("blogger#commentList", true);

        final String kind;
        final boolean paginated;

        Stage(String kind, boolean paginated) {
            this.kind = kind;
            this.paginated = paginated;
        }
    }
}
