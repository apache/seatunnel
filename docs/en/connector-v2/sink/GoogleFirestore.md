# GoogleFirestore

> Google Firestore sink connector

## Description

Write data to Google Firestore

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|    name     |  type  | required | default value |
|-------------|--------|----------|---------------|
| project_id  | string | yes      | -             |
| collection  | string | yes      | -             |
| credentials | string | no       | -             |

### project_id [string]

The unique identifier for a Google Firestore database project.

### collection [string]

The collection of Google Firestore.

### credentials [string]

The credentials of Google Cloud service account, use base64 codec. If not set, need to check the `GOOGLE APPLICATION CREDENTIALS` environment exists.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

```bash
GoogleFirestore {
  project_id = "dummy-project-id",
  collection = "dummy-collection",
  credentials = "dummy-credentials"
}  
```

## Changelog

### next version

- Add Google Firestore Sink Connector

