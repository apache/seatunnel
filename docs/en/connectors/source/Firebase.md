# Firebase

> Firebase source connector

## Description

Read data from Firebase Realtime Database or Cloud Firestore.

## Key Features

- [x] batch
- [ ] stream
- [x] exact-once
- [ ] column projection
- [x] parallelism
- [ ] support user-defined split

## Options

| name | type | required | default value | description |
| :--- | :--- | :--- | :--- | :--- |
| url | string | yes | - | Firebase Database URL |
| credential_path | string | no | - | Path to service account JSON file |
| collection / path | string | yes | - | Target node or collection path |
| common-options | - | no | - | Source plugin common parameters, please refer to [Source Plugin](common-options.md) for details |

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Firebase {
    url = "[https://your-project-id.firebaseio.com](https://your-project-id.firebaseio.com)"
    path = "users"
    result_table_name = "firebase_users"
  }
}

sink {
  Console {}
}