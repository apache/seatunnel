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

package org.apache.seatunnel.connectors.seatunnel.salesforce.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceParameters;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class SalesforceClient implements Closeable {

    private static final String TOKEN_PATH = "/services/oauth2/token";
    private static final String JOBS_QUERY_PATH = "/services/data/%s/jobs/query";
    private static final String JOB_PATH = "/services/data/%s/jobs/query/%s";
    private static final String JOB_RESULTS_PATH = "/services/data/%s/jobs/query/%s/results";
    private static final String DESCRIBE_PATH = "/services/data/%s/sobjects/%s/describe";
    private static final String PLUGIN_NAME = "Salesforce";

    private final SalesforceParameters params;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final boolean sinkMode;
    private String accessToken;
    private String authorizedInstanceUrl;

    public SalesforceClient(SalesforceParameters params) {
        this(params, false);
    }

    /** Create a write client with explicit retries and no credential-forwarding redirects. */
    public static SalesforceClient forSink(SalesforceParameters params) {
        return new SalesforceClient(params, true);
    }

    private SalesforceClient(SalesforceParameters params, boolean sinkMode) {
        this.params = params;
        this.sinkMode = sinkMode;
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(params.getRequestTimeoutMs())
                        .setSocketTimeout(params.getRequestTimeoutMs())
                        .setConnectionRequestTimeout(sinkMode ? params.getRequestTimeoutMs() : -1)
                        .build();
        HttpClientBuilder builder = HttpClients.custom().setDefaultRequestConfig(requestConfig);
        if (sinkMode) {
            builder.disableAutomaticRetries().disableRedirectHandling();
        }
        this.httpClient = builder.build();
    }

    public void authenticate() {
        String tokenUrl = params.getInstanceUrl() + TOKEN_PATH;
        HttpPost post = new HttpPost(tokenUrl);

        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair("grant_type", "password"));
        form.add(new BasicNameValuePair("client_id", params.getClientId()));
        form.add(new BasicNameValuePair("client_secret", params.getClientSecret()));
        form.add(new BasicNameValuePair("username", params.getUsername()));
        form.add(
                new BasicNameValuePair(
                        "password", params.getPassword() + params.getSecurityToken()));

        try {
            post.setEntity(new UrlEncodedFormEntity(form, StandardCharsets.UTF_8));
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int status = response.getStatusLine().getStatusCode();
                String body =
                        sinkMode
                                ? readSinkBody(response)
                                : EntityUtils.toString(
                                        response.getEntity(), StandardCharsets.UTF_8);
                if (status != 200) {
                    throw new SalesforceConnectorException(
                            SalesforceConnectorErrorCode.AUTH_FAILED,
                            "HTTP " + status + (sinkMode ? "" : ": " + body));
                }
                JsonNode json = objectMapper.readTree(body);
                if (sinkMode) {
                    if (json == null
                            || !json.path("access_token").isTextual()
                            || json.path("access_token").asText().isEmpty()
                            || !json.path("instance_url").isTextual()) {
                        throw new IOException("Invalid authentication response");
                    }
                    SalesforceSinkConfig.validateInstanceUrl(json.path("instance_url").asText());
                    if (params.getInstanceUrl().regionMatches(true, 0, "https:", 0, 6)
                            && !json.path("instance_url")
                                    .asText()
                                    .regionMatches(true, 0, "https:", 0, 6)) {
                        throw new IOException("Authentication returned an insecure instance");
                    }
                }
                this.accessToken = json.get("access_token").asText();
                this.authorizedInstanceUrl = json.get("instance_url").asText();
                log.info("Authenticated with Salesforce instance {}", authorizedInstanceUrl);
            }
        } catch (SalesforceConnectorException e) {
            throw e;
        } catch (Exception e) {
            if (sinkMode) {
                throw new SalesforceConnectorException(
                        SalesforceConnectorErrorCode.AUTH_FAILED,
                        "Authentication failed (" + e.getClass().getSimpleName() + ")");
            }
            throw new SalesforceConnectorException(SalesforceConnectorErrorCode.AUTH_FAILED, e);
        }
    }

    /**
     * Upsert one bounded collection with allOrNone=true. Request retries can repeat Salesforce-side
     * effects; this is not a checkpoint transaction or exactly-once protocol.
     */
    public void upsert(
            String objectName,
            String externalIdField,
            List<ObjectNode> records,
            int maxRetries,
            long retryIntervalMs) {
        if (!sinkMode || accessToken == null) {
            throw writeFailure("An authenticated sink client is required");
        }
        SalesforceSinkConfig.requireIdentifier(objectName, "object_name");
        SalesforceSinkConfig.requireIdentifier(externalIdField, "external_id_field");
        if (records.isEmpty()
                || records.size() > 200
                || maxRetries < 0
                || maxRetries > 10
                || retryIntervalMs < 0
                || retryIntervalMs > 60000) {
            throw writeFailure("Invalid upsert batch or retry bounds");
        }
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("allOrNone", true);
        records.forEach(payload.putArray("records")::add);
        String requestBody = payload.toString();
        boolean refreshed = false;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw writeFailure("Interrupted before collection upsert");
            }
            long delay = retryIntervalMs;
            HttpPatch patch =
                    new HttpPatch(
                            authorizedInstanceUrl
                                    + "/services/data/"
                                    + params.getApiVersion()
                                    + "/composite/sobjects/"
                                    + objectName
                                    + "/"
                                    + externalIdField);
            patch.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
            patch.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));
            boolean refresh = false;
            try (CloseableHttpResponse response = httpClient.execute(patch)) {
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    validateUpsertResults(readSinkBody(response), records.size());
                    return;
                }
                if (status == 401 && !refreshed && attempt < maxRetries) {
                    refresh = true;
                } else if ((status == 429
                                || status == 500
                                || status == 502
                                || status == 503
                                || status == 504)
                        && attempt < maxRetries) {
                    delay = retryDelay(response.getFirstHeader("Retry-After"), retryIntervalMs);
                } else {
                    throw writeFailure(
                            "Collection upsert returned HTTP "
                                    + status
                                    + " after "
                                    + (attempt + 1)
                                    + " attempt(s)");
                }
            } catch (IOException e) {
                if (Thread.currentThread().isInterrupted() || attempt == maxRetries) {
                    throw writeFailure(
                            "Collection upsert I/O failed ("
                                    + e.getClass().getSimpleName()
                                    + ") after "
                                    + (attempt + 1)
                                    + " attempt(s)");
                }
            }
            if (refresh) {
                authenticate();
                refreshed = true;
            } else {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw writeFailure("Interrupted while waiting to retry collection upsert");
                }
            }
        }
        throw writeFailure("Collection upsert exhausted retries");
    }

    private void validateUpsertResults(String body, int count) {
        JsonNode results;
        try {
            results = objectMapper.readTree(body);
        } catch (IOException e) {
            throw writeFailure("Invalid collection upsert JSON response");
        }
        if (results == null || !results.isArray() || results.size() != count) {
            throw writeFailure("Collection upsert response count does not match the request");
        }
        List<String> failures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonNode result = results.get(i);
            if (!result.path("success").isBoolean() || !result.path("errors").isArray()) {
                throw writeFailure("Malformed collection result at index " + i);
            }
            if (!result.path("success").asBoolean() || result.path("errors").size() != 0) {
                List<String> codes = new ArrayList<>();
                for (JsonNode error : result.path("errors")) {
                    String code = error.path("statusCode").asText();
                    codes.add(code.matches("[A-Z_]{1,80}") ? code : "UNKNOWN");
                }
                failures.add("index " + i + ": " + codes);
            } else if (!result.path("id").isTextual() || result.path("id").asText().isEmpty()) {
                throw writeFailure("Missing record ID in collection result at index " + i);
            }
        }
        if (!failures.isEmpty()) {
            // API messages can contain field values; report indices and codes, not response bodies.
            throw writeFailure("Collection upsert failed: " + failures);
        }
    }

    private long retryDelay(Header retryAfter, long configuredDelay) {
        if (retryAfter == null) {
            return configuredDelay;
        }
        long delay;
        try {
            delay = Math.multiplyExact(Long.parseLong(retryAfter.getValue().trim()), 1000L);
        } catch (NumberFormatException e) {
            Date date = DateUtils.parseDate(retryAfter.getValue());
            if (date == null) {
                throw writeFailure("Invalid Retry-After response header");
            }
            delay = Math.max(0, date.getTime() - System.currentTimeMillis());
        } catch (ArithmeticException e) {
            throw writeFailure("Retry-After exceeds the bounded retry budget");
        }
        if (delay < 0 || delay > 60000) {
            throw writeFailure("Retry-After exceeds the bounded retry budget");
        }
        return Math.max(delay, configuredDelay);
    }

    private String readSinkBody(CloseableHttpResponse response) throws IOException {
        if (response.getEntity() == null) {
            throw writeFailure("Missing Salesforce response body");
        }
        try (InputStream input = response.getEntity().getContent();
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if (output.size() + read > 1024 * 1024) {
                    throw writeFailure("Salesforce response exceeds the 1 MiB client limit");
                }
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private SalesforceConnectorException writeFailure(String message) {
        return new SalesforceConnectorException(SalesforceConnectorErrorCode.WRITE_FAILED, message);
    }

    public CatalogTable describeObject(String database, String objectName) {
        String url =
                authorizedInstanceUrl
                        + String.format(DESCRIBE_PATH, params.getApiVersion(), objectName);
        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);

        try (CloseableHttpResponse response = httpClient.execute(get)) {
            int status = response.getStatusLine().getStatusCode();
            String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (status != 200) {
                throw new SalesforceConnectorException(
                        SalesforceConnectorErrorCode.DESCRIBE_OBJECT_FAILED,
                        "HTTP " + status + " describing " + objectName + ": " + body);
            }
            return buildCatalogTable(database, objectName, objectMapper.readTree(body));
        } catch (SalesforceConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new SalesforceConnectorException(
                    SalesforceConnectorErrorCode.DESCRIBE_OBJECT_FAILED, e);
        }
    }

    private CatalogTable buildCatalogTable(String database, String objectName, JsonNode describe) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (JsonNode field : describe.get("fields")) {
            String name = field.get("name").asText();
            String sfType = field.get("type").asText();
            SeaTunnelDataType<?> seaType = mapSalesforceType(sfType);
            schemaBuilder.column(PhysicalColumn.of(name, seaType, null, null, true, null, null));
        }
        return CatalogTable.of(
                TableIdentifier.of(PLUGIN_NAME, database, objectName),
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private SeaTunnelDataType<?> mapSalesforceType(String sfType) {
        switch (sfType) {
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "int":
                return BasicType.INT_TYPE;
            case "double":
            case "currency":
            case "percent":
                return BasicType.DOUBLE_TYPE;
            case "date":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "datetime":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "time":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "base64":
                return PrimitiveByteArrayType.INSTANCE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    /**
     * Runs one Bulk API query end-to-end: creates the job, polls until it reaches a terminal state,
     * then streams the result pages to rowConsumer. Rows are emitted as they are parsed rather than
     * buffered, so callers can forward each row to a downstream collector without holding the full
     * result set in memory.
     */
    public void executeBulkQuery(String soql, int columnCount, Consumer<Object[]> rowConsumer) {
        String jobId = createBulkQueryJob(soql);
        waitForJobCompletion(jobId);
        downloadResults(jobId, columnCount, rowConsumer);
    }

    private String createBulkQueryJob(String soql) {
        String url = authorizedInstanceUrl + String.format(JOBS_QUERY_PATH, params.getApiVersion());
        HttpPost post = new HttpPost(url);
        post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
        post.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        post.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());

        try {
            ObjectNode body = objectMapper.createObjectNode();
            body.put("operation", "query");
            body.put("query", soql);
            post.setEntity(
                    new StringEntity(
                            objectMapper.writeValueAsString(body), ContentType.APPLICATION_JSON));

            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int status = response.getStatusLine().getStatusCode();
                String responseBody =
                        EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (status != 200) {
                    throw new SalesforceConnectorException(
                            SalesforceConnectorErrorCode.BULK_JOB_CREATE_FAILED,
                            "HTTP " + status + ": " + responseBody);
                }
                String jobId = objectMapper.readTree(responseBody).get("id").asText();
                log.info("Created Bulk API query job {} for SOQL: {}", jobId, soql);
                return jobId;
            }
        } catch (SalesforceConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new SalesforceConnectorException(
                    SalesforceConnectorErrorCode.BULK_JOB_CREATE_FAILED, e);
        }
    }

    private void waitForJobCompletion(String jobId) {
        String url = authorizedInstanceUrl + String.format(JOB_PATH, params.getApiVersion(), jobId);
        long deadline = System.currentTimeMillis() + params.getJobCompletionTimeoutMs();
        while (true) {
            try {
                Thread.sleep(params.getPollIntervalMs());
                HttpGet get = new HttpGet(url);
                get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
                try (CloseableHttpResponse response = httpClient.execute(get)) {
                    String responseBody =
                            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    String state = objectMapper.readTree(responseBody).get("state").asText();
                    log.debug("Bulk API job {} state: {}", jobId, state);
                    if ("JobComplete".equals(state)) {
                        return;
                    }
                    if ("Failed".equals(state) || "Aborted".equals(state)) {
                        throw new SalesforceConnectorException(
                                SalesforceConnectorErrorCode.BULK_JOB_FAILED,
                                "Job " + jobId + " ended with state: " + state);
                    }
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new SalesforceConnectorException(
                            SalesforceConnectorErrorCode.BULK_JOB_FAILED,
                            "Job "
                                    + jobId
                                    + " did not complete within "
                                    + params.getJobCompletionTimeoutMs()
                                    + "ms");
                }
            } catch (SalesforceConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new SalesforceConnectorException(
                        SalesforceConnectorErrorCode.BULK_JOB_FAILED, e);
            }
        }
    }

    /**
     * Pulls every result page for a completed Bulk API query job, walking forward via the
     * Sforce-Locator header. Each parsed row is pushed to rowConsumer immediately; no whole-job
     * buffering happens at this layer, only one page's CSV body is held at a time.
     */
    private void downloadResults(String jobId, int columnCount, Consumer<Object[]> rowConsumer) {
        String url =
                authorizedInstanceUrl
                        + String.format(JOB_RESULTS_PATH, params.getApiVersion(), jobId);
        String locator = null;

        do {
            String requestUrl = locator == null ? url : url + "?locator=" + locator;
            HttpGet get = new HttpGet(requestUrl);
            get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
            get.setHeader(HttpHeaders.ACCEPT, "text/csv");

            try (CloseableHttpResponse response = httpClient.execute(get)) {
                int status = response.getStatusLine().getStatusCode();
                String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (status != 200) {
                    throw new SalesforceConnectorException(
                            SalesforceConnectorErrorCode.BULK_RESULTS_FAILED,
                            "HTTP " + status + ": " + body);
                }
                Header locatorHeader = response.getFirstHeader("Sforce-Locator");
                locator =
                        (locatorHeader != null && !"null".equals(locatorHeader.getValue()))
                                ? locatorHeader.getValue()
                                : null;
                parseCsvInto(body, columnCount, rowConsumer);
            } catch (SalesforceConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new SalesforceConnectorException(
                        SalesforceConnectorErrorCode.BULK_RESULTS_FAILED, e);
            }
        } while (locator != null);
    }

    /**
     * Parses one Bulk API CSV result page (header row plus data rows) and emits each data row to
     * rowConsumer as a fixed-width Object[]. Missing trailing columns are filled with null and
     * empty cells also become null, so the type-converting reader can distinguish them from real
     * string values.
     */
    private void parseCsvInto(String csv, int columnCount, Consumer<Object[]> rowConsumer)
            throws IOException {
        try (CSVParser parser =
                CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new StringReader(csv))) {
            for (CSVRecord record : parser) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    String val = i < record.size() ? record.get(i) : null;
                    row[i] = (val == null || val.isEmpty()) ? null : val;
                }
                rowConsumer.accept(row);
            }
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
