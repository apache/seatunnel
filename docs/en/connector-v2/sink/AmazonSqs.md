# AmazonSqs

> Amazon SQS sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Amazon SQS

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|          Name           |  Type  | Required | Default |                                                                                                                                                                                                             Description                                                                                                                                                                                                             |
|-------------------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String | Yes      | -       | The Queue URL to read from Amazon SQS.                                                                                                                                                                                                                                                                                                                                                                                              |
| region                  | String | No       | -       | The AWS region for the SQS service                                                                                                                                                                                                                                                                                                                                                                                                  |
| common-options          |        | No       | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                                                                                                                                                                                             |


### url [string]

The URL to write to Amazon SQS.

### region [string]

The region of Amazon SQS.

### queue [string]

The queue of Amazon SQS.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

```bash
amazonsqs {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    queue = "queueName"
  }
```

## Changelog

### next version

- Add Amazon SQS Sink Connector

