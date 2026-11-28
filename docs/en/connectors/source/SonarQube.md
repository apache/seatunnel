<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-http-sonarqube.md';

# SonarQube

> SonarQube Community Build source connector

The `SonarQube` source reads current main-branch measures for one SonarQube Community Build project using [its Web API](https://docs.sonarsource.com/sonarqube-community-build/extension-guide/web-api). It outputs one row for each measure returned by `GET /api/measures/component`.

## Supported features

- Bounded batch, with source parallelism `1`. No historical, incremental or exactly-once reads.
- Explicit project selection. No project discovery, rule or issue export, pull-request analysis, or branch selection.
- Public projects can be read without a token if the server supports anonymous Browse access. A private project needs a user token with Browse access; administrator permission is not required.

## Options

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `base_url` | String | Yes | Base URL, including any context path, for example `https://sonar.example.org/sonarqube`. |
| `project_key` | String | Yes | Exact SonarQube project key, not the display name. |
| `metric_keys` | List of String | Yes | 1 to 50 distinct [metric keys](https://docs.sonarsource.com/sonarqube-community-build/user-guide/code-metrics/metrics-definition). |
| `token_env` | String | No | Name of a worker environment variable containing a SonarQube user token. Requires HTTPS. Don’t place the token in the job file. |

## Output

| Field | Type | Meaning |
| --- | --- | --- |
| `project_key` | STRING | SonarQube project key. |
| `project_name` | STRING | Project display name. |
| `metric_key` | STRING | Metric key from the API. |
| `value` | STRING | Current overall measure value as returned by the API, or null if absent. |
| `best_value` | BOOLEAN | Whether the current value is optimal, or null if unspecified. |
| `measure` | STRING | Full measure as compact JSON. This retains new-code period fields when present. |

A metric that the API omits is not emitted. An empty measures array produces no rows. HTTP errors, redirects and inconsistent server responses fail the job.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  SonarQube {
    base_url = "https://sonar.example.org/sonarqube"
    project_key = "example:app"
    metric_keys = ["ncloc", "coverage", "new_coverage"]
    token_env = "SONARQUBE_READ_TOKEN"
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
