import ChangeLog from '../changelog/connector-blackhole.md';


# BlackHole

> BlackHole sink connector

## Description

A sink connector that discards all records it receives. This can be useful for performance testing or when you want to ignore certain data.

## Key features

* Discard all data

## Options

No configuration options required.

## Example

Simple example, this example defines a SeaTunnel synchronization task that reads data from a FakeSource and discards it using BlackHole sink.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Tom", 25]
      }
    ]
  }
}

sink {
  BlackHole {}
}
```