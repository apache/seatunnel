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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.HttpUrl;
import okhttp3.MediaType;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Extracts Box's first-party text representation for a verified file.
 *
 * <p>Generation is polled on the API origin. Only the Box content origin can receive the subsequent
 * download request and its credential.
 */
final class BoxDownload {
    private static final String PLACEHOLDER = "{+asset_path}";
    private static final String FORMAT = "extracted_text";

    private BoxDownload() {}

    /**
     * Fetches generated UTF-8 text without sending the token to a supplied arbitrary URL.
     *
     * <p>The service may need an initial info request to start generation. Later polls stay within
     * the same acquisition deadline and do not expose the representation URL.
     */
    static String text(
            BoxHttp client,
            BoxHttp.Acquisition acquisition,
            BoxConfig config,
            BoxWork work,
            HttpUrl fileUrl,
            String version)
            throws BoxReadException, InterruptedException {
        String operation = work.stage.operation;
        JsonNode response =
                acquisition.hint(
                        fileUrl.newBuilder()
                                .addQueryParameter("fields", "name,representations")
                                .build(),
                        operation);
        if (!work.file.equals(response.path("id").asText())) {
            throw work.failure("representation response does not match the requested file");
        }
        JsonNode representation = select(response.path("representations"), work);
        if (representation == null) {
            return "";
        }

        String state = state(representation, work);
        String template = optionalTemplate(representation, work);
        if (!"success".equals(state)) {
            HttpUrl statusUrl =
                    statusUrl(client, representation.path("info").path("url"), version, work);
            for (int poll = 0; poll < config.maxPolls; poll++) {
                BoxHttp.Result status = acquisition.poll(statusUrl, operation);
                if (status.status == 202 && status.bytes.length == 0) {
                    state = "pending";
                } else {
                    JsonNode body = BoxHttp.asJson(status, operation);
                    state = state(body, work);
                    String updatedTemplate = optionalTemplate(body, work);
                    if (updatedTemplate != null) {
                        template = updatedTemplate;
                    }
                }
                if ("success".equals(state)) {
                    break;
                }
                if (poll + 1 < config.maxPolls) {
                    BoxHttp.pause(
                            operation, acquisition.deadline, Math.max(200L, status.retryDelay));
                }
            }
            if (!"success".equals(state)) {
                throw work.failure("max_representation_polls exhausted");
            }
        }

        HttpUrl download = contentUrl(client, template, version, work);
        BoxHttp.Result data = acquisition.content(download, operation);
        MediaType type = data.type;
        if (type == null
                || !("text".equals(type.type()) && "plain".equals(type.subtype()))
                        && !("application".equals(type.type())
                                && "octet-stream".equals(type.subtype()))) {
            throw work.failure("expected UTF-8 text representation content");
        }
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data.bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw work.failure("decoded representation content is not UTF-8");
        }
    }

    private static JsonNode select(JsonNode source, BoxWork work) throws BoxReadException {
        if (source.isMissingNode() || source.isNull()) {
            return null;
        }
        JsonNode entries = source.path("entries");
        if (!entries.isArray()) {
            throw work.failure("invalid Box representations array");
        }
        JsonNode result = null;
        for (JsonNode entry : entries) {
            if (!entry.isObject()) {
                throw work.failure("invalid Box representation entry");
            }
            if (FORMAT.equals(entry.path("representation").asText())) {
                if (result != null) {
                    throw work.failure("multiple extracted text representations for one file");
                }
                result = entry;
            }
        }
        return result;
    }

    private static String state(JsonNode representation, BoxWork work) throws BoxReadException {
        String state = representation.path("status").path("state").asText();
        switch (state) {
            case "success":
            case "none":
            case "pending":
            case "viewable":
                return state;
            case "error":
            case "failed":
                throw work.failure("Box failed to generate the extracted text representation");
            default:
                throw work.failure("unexpected Box representation status");
        }
    }

    private static String optionalTemplate(JsonNode body, BoxWork work) throws BoxReadException {
        JsonNode field = body.path("content").path("url_template");
        if (field.isMissingNode() || field.isNull()) {
            return null;
        }
        if (!field.isTextual() || field.textValue().length() > 8192) {
            throw work.failure("invalid Box representation download template");
        }
        return field.textValue();
    }

    private static HttpUrl statusUrl(BoxHttp client, JsonNode field, String version, BoxWork work)
            throws BoxReadException {
        HttpUrl url = absolute(field.textValue(), work, "invalid Box representation status URL");
        String start = "/2.0/internal_files/" + work.file + "/versions/";
        if (!client.allowedApi(url)
                || !isKnownPath(url, start, "/representations/" + FORMAT, version)
                || url.querySize() != 0) {
            throw work.failure(
                    "representation status URL does not match the API origin or file identity");
        }
        return url;
    }

    private static HttpUrl contentUrl(BoxHttp client, String template, String version, BoxWork work)
            throws BoxReadException {
        String reason = "invalid Box representation download template";
        if (template == null) {
            throw work.failure(reason);
        }
        int place = template.indexOf(PLACEHOLDER);
        int query = template.indexOf('?');
        if (place < 0
                || place != template.lastIndexOf(PLACEHOLDER)
                || (query >= 0 && query < place)) {
            throw work.failure(reason);
        }
        HttpUrl url = absolute(template.replace(PLACEHOLDER, ""), work, reason);
        if (!client.allowedContent(url) || "/".equals(url.encodedPath())) {
            throw work.failure("representation download origin is outside the Box allowlist");
        }

        // Validate the object ID for the documented path when present. Box also documents opaque
        // representation URLs, which remain usable within a trusted content origin.
        String prefix = "/api/2.0/internal_files/";
        if (url.encodedPath().startsWith(prefix)
                && !isKnownPath(
                        url,
                        prefix + work.file + "/versions/",
                        "/representations/" + FORMAT + "/content/",
                        version)) {
            throw work.failure(
                    "representation download URL does not refer to the requested file version");
        }
        return url;
    }

    private static boolean isKnownPath(HttpUrl url, String prefix, String suffix, String version) {
        String path = url.encodedPath();
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) {
            return false;
        }
        int end = path.length() - suffix.length();
        if (end <= prefix.length()) {
            return false;
        }
        String candidate = path.substring(prefix.length(), end);
        return version == null ? candidate.matches("[0-9]{1,64}") : version.equals(candidate);
    }

    private static HttpUrl absolute(String uri, BoxWork work, String diagnostic)
            throws BoxReadException {
        if (uri != null && uri.length() <= 8192 && !uri.contains("\\")) {
            try {
                URI address = URI.create(uri);
                if ("https".equals(address.getScheme())
                        && address.getHost() != null
                        && address.getRawUserInfo() == null
                        && address.getRawFragment() == null) {
                    return HttpUrl.get(address);
                }
            } catch (IllegalArgumentException failure) {
                // The supplied representation URL may include a secret.
            }
        }
        throw work.failure(diagnostic);
    }
}
