# Connector Check Command Usage

## Command Entrypoint

```shell
bin/seatunnel-connector.sh
```

## Options

```text
Usage: seatunnel-connector.sh [options]
  Options:
    -h, --help         Show the usage message
    -l, --list         List all supported plugins(sources, sinks, transforms) 
                       (default: false)
    -o, --option-rule  Get option rule of the plugin by the plugin 
                       identifier(connector name or transform name)
    -pt, --plugin-type SeaTunnel plugin type, support [source, sink, 
                       transform] 
```

## Example

```shell
# List all supported connectors(sources and sinks) and transforms
bin/seatunnel-connector.sh -l
# List all supported sinks
bin/seatunnel-connector.sh -l -pt sink
# Get option rule of the connector or transform by the name
bin/seatunnel-connector.sh -o Paimon
# Get option rule of paimon sink
bin/seatunnel-connector.sh -o Paimon -pt sink
```

