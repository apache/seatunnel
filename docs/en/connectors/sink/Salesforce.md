import ChangeLog from '../changelog/connector-salesforce.md';

# Salesforce

> Salesforce REST sObject Collections upsert sink

## Description

Writes one input table to one existing Salesforce object using its external ID field.
The connector extends the existing Salesforce module without changing source options or data-reading behavior.
It does not create objects, fields, external IDs, or connected apps.

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Key Features

- [x] batch
- [x] stream
- [ ] exactly-once
- [ ] cdc
- [ ] support multiple table write

Streaming support is limited to `INSERT` and `UPDATE_AFTER` rows. This is not a CDC sink:
`UPDATE_BEFORE` and `DELETE` fail the job. See [Delivery Semantics](#delivery-semantics)
before connecting a changelog source.

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| client_id | string | yes | - |
| client_secret | string | yes | - |
| username | string | yes | - |
| password | string | yes | - |
| security_token | string | no | empty string |
| instance_url | string | yes | - |
| api_version | string | no | v59.0 |
| object_name | string | yes | - |
| external_id_field | string | yes | - |
| batch_size | int | no | 200 |
| batch_max_bytes | int | no | 1048576 |
| request_timeout_ms | int | no | 60000 |
| max_retries | int | no | 3 |
| retry_interval_ms | long | no | 1000 |
| common-options | | no | - |

### Authentication and Permissions

Uses the existing source connector's OAuth username-password flow. Your org must permit that flow;
the integration user needs API access, object create/update access, and write access to all mapped fields.
The security token is appended to the password, as in the source connector.

Use an HTTPS login or org origin for `instance_url`, for example
`https://login.salesforce.com` or your sandbox login origin. Do not include a trailing slash,
path, embedded credentials, query, or fragment. HTTP is accepted for explicitly configured
trusted test endpoints but transmits credentials in plaintext; do not use it for production.
The authenticated instance URL is used for data requests. Redirects and HTTPS-to-HTTP
authentication downgrades are rejected in the sink path.

`client_secret` and `security_token` are automatically masked in parsed-configuration logs.
This is log masking, not configuration encryption. Keep credentials out of checked-in job files.

### Object and Field Mapping

`object_name` is the Salesforce API object name, such as `Account`.
Input field names must match writable Salesforce API fields. Relationships, nested objects,
arrays, and automatic schema creation are not supported.

`external_id_field` must exist in both Salesforce and the input schema. Configure it as an
external ID and preferably Unique in Salesforce. Each row must provide a non-null, non-blank
STRING, integer, or DECIMAL key. Salesforce `Id` is not accepted as the external ID or an input field.
The connector does not verify remote field metadata before sending; Salesforce validation or
permission errors fail the job. Null values in other columns explicitly clear those fields.

BOOLEAN and numeric values remain native JSON values. DECIMAL is serialized without conversion
to double; the target field's configured precision still applies. Non-finite floats are rejected.
DATE uses ISO dates, TIME uses UTC time with milliseconds, TIMESTAMP without a zone is interpreted
as UTC, and TIMESTAMP_TZ preserves its explicit offset. Sub-millisecond temporal values are
rejected instead of truncated. BYTES uses Base64. Salesforce determines whether each value
is compatible with the configured target field.

### Batching and Failures

`batch_size` is 1-200 records per request. `batch_max_bytes` is the UTF-8 JSON request bound,
including the envelope, from 128 to 8388608 bytes. This is a client-side bound, not a promise
about Salesforce's request limits. A single oversized record fails before sending.

Batches use `allOrNone = true`. Every record result must report success; malformed responses,
missing results, or any record error fail the task and prevent checkpoint completion.
Error diagnostics include result indices and Salesforce status codes, not record values or
raw response bodies. Per-record failures are not retried locally, including lock/validation errors.

`max_retries` is 0-10 additional attempts after an I/O failure or HTTP 429, 500, 502, 503, or 504.
One HTTP 401 may refresh authentication within the same retry budget. Other HTTP errors fail
immediately. Authentication failures themselves are not retried automatically. This avoids
repeated login attempts with rejected credentials. Quota errors returned as HTTP 403 also fail
immediately. Retry-After is honored up to 60 seconds;
larger or invalid values fail the task rather than retrying earlier than the service permits.
`retry_interval_ms` is 0-60000. `request_timeout_ms` is a positive connection/pool/socket timeout,
not a whole-job deadline. Choose parallelism and retry settings within your org's API quota.
Flushing is synchronous, so size the checkpoint timeout to allow the complete flush, including
request timeouts, retry delays, and any authentication refresh. A single request timeout is not
the total flush budget, especially during sustained throttling or service failures.

The writer flushes on count/byte bounds, checkpoint preparation, and normal close. It also
registers the engine flush callback. Zeta can enable periodic flushing through
`env.sink.flush.interval` (milliseconds; 0 disables it by default). Engines without that callback
rely on count/checkpoint/end of input. Streaming jobs should enable periodic checkpoints to
bound low-volume delivery latency.

## Delivery Semantics

Only `INSERT` and `UPDATE_AFTER` are accepted; `DELETE` and `UPDATE_BEFORE` fail rather than
being ignored. The sink does not reconcile deletes or external-ID changes. Silently discarding
before-images could hide an unsupported changelog pipeline and leave old Salesforce records
behind when a key changes.

Delivery is at-least-once with a replay-capable source and checkpointing. An uncertain HTTP
outcome or task recovery can repeat an upsert and re-run Salesforce triggers/flows. This is not
an exactly-once or distributed-transaction sink; earlier successful requests cannot be rolled back.

Repeated external IDs within one writer are flushed in input order in separate requests.
For ordered updates to the same key, use parallelism 1 or ensure all updates for that key are
routed in order to one writer. There is no global ordering across writers or recovery attempts.
Delete, insert-only, Bulk API ingestion, and multi-object routing are outside this connector slice.

## Task Example

Create `External_Id__c` on Account as a unique external ID and supply the following input columns:

```hocon
sink {
  Salesforce {
    instance_url = "https://login.salesforce.com"
    client_id = "${SALESFORCE_CLIENT_ID}"
    client_secret = "${SALESFORCE_CLIENT_SECRET}"
    username = "${SALESFORCE_USERNAME}"
    password = "${SALESFORCE_PASSWORD}"
    security_token = "${SALESFORCE_SECURITY_TOKEN}"
    object_name = "Account"
    external_id_field = "External_Id__c"
    batch_size = 200
  }
}
```

Use your deployment's substitution mechanism to supply these placeholders.
The input schema should include `External_Id__c = string` and `Name = string`.
See [Sink Common Options](../common-options/sink-common-options.md) for shared parameters.

## Changelog

<ChangeLog />
