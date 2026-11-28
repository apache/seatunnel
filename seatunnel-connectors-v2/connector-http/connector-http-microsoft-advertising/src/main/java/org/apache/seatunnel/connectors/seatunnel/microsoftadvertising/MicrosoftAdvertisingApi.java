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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Kind;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Resource;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSplit.Account;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.BULK_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.CAMPAIGN_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.CUSTOMER_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.REPORT_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.SEARCH_SIZE;

/**
 * Microsoft Entra authentication and provider-native Microsoft Advertising v13 operations.
 *
 * <p>The request path fixes its credential authority and every account-scoped operation uses the
 * owning customer returned by account discovery.
 */
final class MicrosoftAdvertisingApi {
    private static final String ACCOUNT_PATH = "/CustomerManagement/v13/Accounts/Search";
    private static final String USER_PATH = "/CustomerManagement/v13/User/Query";
    private static final String CAMPAIGN_PATH =
            "/CampaignManagement/v13/Campaigns/QueryByAccountId";
    private static final String GROUP_PATH = "/CampaignManagement/v13/AdGroups/QueryByCampaignId";
    private static final String AD_PATH = "/CampaignManagement/v13/Ads/QueryByAdGroupId";
    private static final String REPORT_SUBMIT = "/Reporting/v13/GenerateReport/Submit";
    private static final String REPORT_POLL = "/Reporting/v13/GenerateReport/Poll";
    private static final String BULK_SUBMIT = "/Bulk/v13/Campaigns/DownloadByAccountIds";
    private static final String BULK_POLL = "/Bulk/v13/BulkDownloadStatus/Query";
    private static final List<String> AD_TYPES =
            List.of(
                    "AppInstall",
                    "DynamicSearch",
                    "ExpandedText",
                    "Hotel",
                    "Image",
                    "Product",
                    "ResponsiveAd",
                    "ResponsiveSearch",
                    "Text");
    private static final Map<Integer, String> REAUTH =
            Map.of(
                    50078, "tenant MFA requires a new interactive sign-in",
                    70043, "tenant conditional access requires a new sign-in",
                    700082, "the OAuth refresh token expired after inactivity",
                    50173, "the OAuth grant was revoked");

    private final MicrosoftAdvertisingConfig config;
    private final MicrosoftAdvertisingHttp http;

    // Tokens are confined to the API worker and never enter a split or sink row.
    private String refreshToken;
    private String accessToken;
    private long expiresAt;

    MicrosoftAdvertisingApi(MicrosoftAdvertisingConfig config, MicrosoftAdvertisingHttp http) {
        this.config = config;
        this.http = http;
        refreshToken = config.refreshToken;
    }

    String user() throws InterruptedException {
        JsonNode response = read(CUSTOMER_HOST, USER_PATH, null, object().putNull("UserId"));
        return id(response.path("User").path("Id"), "authenticated user");
    }

    Page accounts(String user, int predicateIndex, int page) throws InterruptedException {
        ObjectNode body = object();
        body.putObject("PageInfo").put("Index", page).put("Size", SEARCH_SIZE);
        body.putArray("Ordering").addObject().put("Field", "Id").put("Order", "Ascending");
        body.put("ReturnAdditionalFields", "TaxCertificate,AccountMode");
        ObjectNode predicate = body.putArray("Predicates").addObject();
        if (config.accountNames.isEmpty()) {
            predicate.put("Field", "UserId").put("Operator", "Equals").put("Value", user);
        } else {
            MicrosoftAdvertisingConfig.AccountName criterion =
                    config.accountNames.get(predicateIndex);
            predicate
                    .put("Field", "AccountName")
                    .put("Operator", criterion.operator)
                    .put("Value", criterion.name);
        }
        return page(read(CUSTOMER_HOST, ACCOUNT_PATH, null, body), "Accounts", SEARCH_SIZE);
    }

    Page campaigns(Account account) throws InterruptedException {
        ObjectNode body =
                object().put("AccountId", account.id)
                        .put(
                                "CampaignType",
                                "App,Audience,DynamicSearchAds,Hotel,ObjectiveBased,PerformanceMax,Search,Shopping")
                        .put(
                                "ReturnAdditionalFields",
                                "AdScheduleUseSearcherTimeZone,BidStrategyId,CpvCpmBiddingScheme,DynamicDescriptionSetting,DynamicFeedSetting,MaxConversionValueBiddingScheme,MultimediaAdsBidAdjustment,TargetImpressionShareBiddingScheme,TargetSetting,VerifiedTrackingSetting");
        return page(read(CAMPAIGN_HOST, CAMPAIGN_PATH, account, body), "Campaigns", 100000);
    }

    Page groups(Account account, String campaign) throws InterruptedException {
        ObjectNode body =
                object().put("CampaignId", campaign)
                        .put(
                                "ReturnAdditionalFields",
                                "AdGroupType,AdScheduleUseSearcherTimeZone,CpmBid,CpvBid,MultimediaAdsBidAdjustment");
        return page(read(CAMPAIGN_HOST, GROUP_PATH, account, body), "AdGroups", 100000);
    }

    Page ads(Account account, String group) throws InterruptedException {
        ObjectNode body =
                object().put("AdGroupId", group)
                        .put(
                                "ReturnAdditionalFields",
                                "ImpressionTrackingUrls,Videos,LongHeadlines");
        ArrayNode types = body.putArray("AdTypes");
        AD_TYPES.forEach(types::add);
        return page(read(CAMPAIGN_HOST, AD_PATH, account, body), "Ads", 100000);
    }

    String submit(
            Resource resource, Account account, MicrosoftAdvertisingSplit split, Instant delta)
            throws InterruptedException {
        boolean bulk = resource.kind == Kind.BULK;
        if (resource.predefinedTime != null
                && !LocalDate.now(MicrosoftAdvertisingConfig.REPORT_ZONE).equals(split.asOf)) {
            throw new MicrosoftAdvertisingFailure(
                    "predefined custom reporting period moved since the scan started");
        }
        ObjectNode body =
                bulk
                        ? bulkRequest(resource, account, delta)
                        : reportRequest(resource, account, split);
        long deadline = MicrosoftAdvertisingHttp.deadline(config.requestTimeout);
        JsonNode result =
                post(
                        bulk ? BULK_HOST : REPORT_HOST,
                        bulk ? BULK_SUBMIT : REPORT_SUBMIT,
                        account,
                        body,
                        true,
                        deadline);
        JsonNode requestId = result.path(bulk ? "DownloadRequestId" : "ReportRequestId");
        if (!requestId.isTextual()
                || !MicrosoftAdvertisingConfig.opaque(requestId.textValue(), 40)) {
            throw new MicrosoftAdvertisingFailure(
                    "async submit returned no valid job identifier; outcome may be unknown");
        }
        return requestId.textValue();
    }

    private ObjectNode bulkRequest(Resource resource, Account account, Instant delta) {
        ObjectNode request = object();
        request.putArray("AccountIds").add(account.id);
        request.putArray("DownloadEntities").add(resource.requestType);
        request.put("DataScope", "EntityData")
                .put("DownloadFileType", "Csv")
                .put("FormatVersion", "6.0")
                .put("CompressionType", "GZip");
        if (delta == null) {
            request.putNull("LastSyncTimeInUTC");
        } else {
            request.put("LastSyncTimeInUTC", delta.truncatedTo(ChronoUnit.MILLIS).toString());
        }
        return request;
    }

    private ObjectNode reportRequest(
            Resource resource, Account account, MicrosoftAdvertisingSplit split) {
        ObjectNode body = object();
        ObjectNode request =
                body.putObject("ReportRequest")
                        .put("ExcludeColumnHeaders", false)
                        .put("ExcludeReportFooter", true)
                        .put("ExcludeReportHeader", true)
                        .put("Format", "Csv")
                        .put("FormatVersion", "2.0")
                        .put(
                                "ReportName",
                                resource.requestType.substring(
                                        0, resource.requestType.length() - 7))
                        .put("ReturnOnlyCompleteData", false)
                        .put("Type", resource.requestType);
        if (resource.aggregation != null) {
            request.put("Aggregation", resource.aggregation);
        }
        ArrayNode columns = request.putArray("Columns");
        resource.columns.forEach(columns::add);
        request.putObject("Scope").putArray("AccountIds").add(account.id);
        if (resource.reportTime) {
            ObjectNode time = request.putObject("Time");
            if (resource.predefinedTime != null) {
                time.put("PredefinedTime", resource.predefinedTime);
            } else {
                date(time.putObject("CustomDateRangeStart"), reportStart(resource, split));
                date(time.putObject("CustomDateRangeEnd"), split.end);
            }
            time.put("ReportTimeZone", "GreenwichMeanTimeDublinEdinburghLisbonLondon");
        }
        return body;
    }

    static LocalDate reportStart(Resource resource, MicrosoftAdvertisingSplit split) {
        LocalDate oldest = resource.oldest(split.asOf);
        return oldest.isAfter(split.start) ? oldest : split.start;
    }

    private static void date(ObjectNode out, LocalDate date) {
        out.put("Day", date.getDayOfMonth())
                .put("Month", date.getMonthValue())
                .put("Year", date.getYear());
    }

    Poll poll(Resource resource, Account account, String jobId) throws InterruptedException {
        boolean bulk = resource.kind == Kind.BULK;
        JsonNode body =
                read(
                        bulk ? BULK_HOST : REPORT_HOST,
                        bulk ? BULK_POLL : REPORT_POLL,
                        account,
                        object().put(bulk ? "RequestId" : "ReportRequestId", jobId));
        if (bulk) {
            String status = body.path("RequestStatus").asText("");
            switch (status) {
                case "InProgress":
                    return new Poll(PollStatus.PENDING, null);
                case "FailedFullSyncRequired":
                    return new Poll(PollStatus.FULL_REQUIRED, null);
                case "Completed":
                    if (!errors(body, true).isEmpty()) {
                        throw new MicrosoftAdvertisingFailure("completed bulk job returned errors");
                    }
                    return new Poll(PollStatus.READY, location(body.path("ResultFileUrl"), false));
                case "Failed":
                    throw new MicrosoftAdvertisingFailure("bulk download job failed");
                default:
                    throw new MicrosoftAdvertisingFailure("invalid bulk download job status");
            }
        }
        JsonNode status = body.path("ReportRequestStatus");
        switch (status.path("Status").asText("")) {
            case "Pending":
                return new Poll(PollStatus.PENDING, null);
            case "Success":
                return new Poll(PollStatus.READY, location(status.path("ReportDownloadUrl"), true));
            case "Error":
                throw new MicrosoftAdvertisingFailure("report job failed");
            default:
                throw new MicrosoftAdvertisingFailure("invalid report job status");
        }
    }

    private static String location(JsonNode value, boolean canBeEmpty) {
        if (canBeEmpty && (value.isMissingNode() || value.isNull())) {
            return null;
        }
        if (!value.isTextual() || !MicrosoftAdvertisingConfig.opaque(value.textValue(), 16384)) {
            throw new MicrosoftAdvertisingFailure("invalid or missing signed download URL");
        }
        return value.textValue();
    }

    private JsonNode read(String host, String operation, Account account, JsonNode input)
            throws InterruptedException {
        return post(
                host,
                operation,
                account,
                input,
                false,
                MicrosoftAdvertisingHttp.deadline(config.requestTimeout));
    }

    private JsonNode post(
            String host,
            String operation,
            Account account,
            JsonNode input,
            boolean create,
            long deadline)
            throws InterruptedException {
        boolean refreshed = false;
        for (int retry = 0; ; ) {
            ensureAccess(deadline);
            MicrosoftAdvertisingHttp.Reply reply =
                    http.post(host, operation, accessToken, account, input, create, deadline);
            List<Integer> codes = errors(reply.body, !operation.equals(BULK_POLL));
            if (reply.status == 401
                    || !codes.isEmpty() && codes.stream().allMatch(code -> code == 109)) {
                if (refreshed) {
                    throw new MicrosoftAdvertisingFailure(
                            "authorization expired after one token renewal");
                }
                refreshed = true;
                accessToken = null;
                continue;
            }
            if (!codes.isEmpty()
                    && codes.stream().allMatch(code -> Set.of(117, 4204).contains(code))) {
                if (retry == config.retries) {
                    throw new MicrosoftAdvertisingFailure("provider rate limit retries exhausted");
                }
                http.backoff(reply.retryAfter, retry++, deadline);
                continue;
            }
            if (!codes.isEmpty()) {
                throw new MicrosoftAdvertisingFailure(
                        "v13 rejected the request with provider error code " + codes.get(0));
            }
            if (reply.status != 200) {
                throw new MicrosoftAdvertisingFailure("v13 request returned HTTP " + reply.status);
            }
            return reply.body;
        }
    }

    private void ensureAccess(long deadline) throws InterruptedException {
        if (accessToken != null && expiresAt - System.nanoTime() > 0) {
            return;
        }
        String grant =
                "grant_type=refresh_token"
                        + "&client_id="
                        + form(config.clientId)
                        + "&refresh_token="
                        + form(refreshToken)
                        + "&scope="
                        + form("https://ads.microsoft.com/msads.manage offline_access");
        if (!config.clientSecret.isEmpty()) {
            grant += "&client_secret=" + form(config.clientSecret);
        }
        MicrosoftAdvertisingHttp.Reply reply =
                http.token(grant.getBytes(StandardCharsets.US_ASCII), deadline);
        if (reply.status != 200 || reply.body != null && reply.body.hasNonNull("error")) {
            throw new MicrosoftAdvertisingFailure(tokenError(reply));
        }
        JsonNode token = reply.body.path("access_token");
        JsonNode replacement = reply.body.path("refresh_token");
        JsonNode expires = reply.body.path("expires_in");
        if (!token.isTextual()
                || !MicrosoftAdvertisingConfig.headerToken(token.textValue())
                || !"Bearer".equalsIgnoreCase(reply.body.path("token_type").asText(""))
                || !expires.isIntegralNumber()
                || !expires.canConvertToInt()
                || expires.intValue() <= 0
                || expires.intValue() > 86400
                || !replacement.isMissingNode()
                        && (!replacement.isTextual()
                                || !MicrosoftAdvertisingConfig.opaque(
                                        replacement.textValue(), 16384))) {
            throw new MicrosoftAdvertisingFailure("invalid Entra token response");
        }
        if (replacement.isTextual()) {
            refreshToken = replacement.textValue();
        }
        long ttl = TimeUnit.SECONDS.toNanos(expires.intValue());
        expiresAt = System.nanoTime() + ttl - Math.min(ttl / 10, TimeUnit.SECONDS.toNanos(30));
        accessToken = token.textValue();
    }

    private static String tokenError(MicrosoftAdvertisingHttp.Reply reply) {
        if (reply.body != null) {
            JsonNode codes = reply.body.path("error_codes");
            if (codes.isArray()) {
                for (JsonNode value : codes) {
                    String reason = REAUTH.get(value.asInt(-1));
                    if (reason != null) {
                        return "Entra OAuth failed: " + reason;
                    }
                }
            }
            if ("invalid_grant".equals(reply.body.path("error").asText(""))) {
                return "Entra OAuth grant is invalid; obtain a new refresh token";
            }
        }
        return "Entra token exchange returned HTTP " + reply.status;
    }

    private static String form(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static List<Integer> errors(JsonNode document, boolean general) {
        List<Integer> codes = new ArrayList<>();
        if (document == null) {
            return codes;
        }
        for (String field :
                general
                        ? List.of("OperationErrors", "AdApiErrors", "BatchErrors", "Errors")
                        : List.of("OperationErrors", "AdApiErrors", "BatchErrors")) {
            JsonNode entries = document.path(field);
            if (entries.isMissingNode() || entries.isNull()) {
                continue;
            }
            if (!entries.isArray()) {
                throw new MicrosoftAdvertisingFailure("invalid v13 error envelope");
            }
            for (JsonNode error : entries) {
                JsonNode code = error.path("Code");
                codes.add(
                        code.canConvertToInt() && code.isIntegralNumber() && code.intValue() > 0
                                ? code.intValue()
                                : 0);
            }
        }
        return codes;
    }

    private Page page(JsonNode response, String field, int maximum) {
        JsonNode collection = response.path(field);
        if (collection.isNull()) {
            collection = http.json.createArrayNode();
        }
        if (!collection.isArray() || collection.size() > maximum) {
            throw new MicrosoftAdvertisingFailure("invalid or oversized " + field + " response");
        }
        List<ObjectNode> rows = new ArrayList<>(collection.size());
        for (JsonNode item : collection) {
            if (!item.isObject()) {
                throw new MicrosoftAdvertisingFailure("invalid row in " + field + " response");
            }
            id(item.path("Id"), field + " item");
            rows.add((ObjectNode) item);
        }
        try {
            String digest =
                    MicrosoftAdvertisingConfig.hex(
                            MicrosoftAdvertisingConfig.digest()
                                    .digest(http.json.writeValueAsBytes(collection)));
            return new Page(rows, digest);
        } catch (JsonProcessingException ex) {
            throw new MicrosoftAdvertisingFailure("could not identify the " + field + " response");
        }
    }

    static String id(JsonNode value, String scope) {
        String id = value.isIntegralNumber() || value.isTextual() ? value.asText() : "";
        if (id.matches("[1-9][0-9]{0,18}")) {
            try {
                Long.parseLong(id);
                return id;
            } catch (NumberFormatException ex) {
                // Values outside the v13 Long domain are malformed identifiers.
            }
        }
        throw new MicrosoftAdvertisingFailure("invalid " + scope + " identifier");
    }

    ObjectNode object() {
        return http.json.createObjectNode();
    }

    /**
     * Object-array response and content fingerprint used by the restartable traversal.
     *
     * <p>No item was emitted when the page was constructed.
     */
    static final class Page {
        final List<ObjectNode> rows;
        final String digest;

        Page(List<ObjectNode> rows, String digest) {
            this.rows = List.copyOf(rows);
            this.digest = digest;
        }
    }

    /**
     * Normalized states shared by the two otherwise independent polling protocols.
     *
     * <p>Only the bulk service can require a definite full synchronization fallback.
     */
    enum PollStatus {
        PENDING,
        READY,
        FULL_REQUIRED
    }

    /**
     * One transient polling result holding a short-lived URL if the file is ready.
     *
     * <p>The reader never places this result or its URL in a checkpoint.
     */
    static final class Poll {
        final PollStatus status;
        final String location;

        Poll(PollStatus status, String location) {
            this.status = status;
            this.location = location;
        }
    }
}
