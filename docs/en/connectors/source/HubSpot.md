import ChangeLog from '../changelog/connector-http-hubspot.md';

# HubSpot

> HubSpot Source Connector

## Description

Read data from HubSpot CRM using the V3 REST API.
Supported objects: Contacts, Companies, Deals, etc.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| access_token | String | Yes | - | The Private App Access Token (Bearer Token) from HubSpot Developers. |
| object_type | String | No | contacts | The HubSpot object type to query (e.g., `contacts`, `companies`, `deals`). |
| common-options | config | No | - | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

### access_token [String]

HubSpot Private App Access Token.
See: [HubSpot Private Apps Guide](https://developers.hubspot.com/docs/api/private-apps)

### object_type [String]

The CRM object to fetch.
Supported values: `contacts`, `companies`, `deals`, `products`, `tickets`, `quotes`, etc.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  HubSpot {
    access_token = "pat-na1-..."
    object_type = "contacts"
  }
}

sink {
  Console {}
}