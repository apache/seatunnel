import ChangeLog from '../changelog/connector-http-hubspot.md';

# HubSpot

> HubSpot Source Connector

## Description

Read data from HubSpot CRM using the V3 REST API.
Supported objects: Contacts, Companies, Deals, etc.

## Key features

| Feature      | Supported |
| :----------- | :-------- |
| Batch        | [x]       |
| Stream       | [ ]       |
| Exactly-Once | [ ]       |
| Parallelism  | [ ]       |

## Description

Read data from HubSpot CRM API. The connector automatically constructs the API URL based on the `object_type` provided, and handles Bearer token authentication.

## Options

| name         | type   | required | default value |
| ------------ | ------ | -------- | ------------- |
| access_token | string | yes      | -             |
| object_type  | string | no       | contacts      |
| url          | string | no       | -             |

**Note:** If `url` is not provided, the connector will automatically query `https://api.hubapi.com/crm/v3/objects/{object_type}`.
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
```
## Changelog

<ChangeLog />
