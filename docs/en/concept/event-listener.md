# Event Listener

## Introduction

The SeaTunnel provides a rich event listening feature that allows you to manage the status at which data is synchronized.
This functionality is crucial when you need to listen job running status.
This document will guide you through the usage of these parameters and how to leverage them effectively.

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Configuration

To use the speed control feature, you need to configure engine config.

### Zeta Engine

Example config in your config file(seatunnel.yaml):

```
seatunnel:
  engine:
    event-report-http:
      url: "http://example.com:1024/event/report"
      headers:
        Content-Type: application/json
```

### Flink/Spark Engine

You can define the implementation class of `org.apache.seatunnel.api.event.EventListener` interface and add to the classpath to automatically load it through SPI.

Example: `org.apache.seatunnel.api.event.LoggingEventHandler`
