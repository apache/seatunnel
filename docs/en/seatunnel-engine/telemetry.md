---

sidebar_position: 8
-------------------

# Telemetry

Integrating `OpenTelemetry` through `Prometheus-exports` can better seamlessly connect to related monitoring platforms such as Prometheus and Grafana, improving the ability to monitor and alarm of the Seatunnel cluster.

You can configure the port exposed by the telemetry server in the `seatunnel.yaml` file.

The following is an example declarative configuration.

```yaml
seatunnel:
  engine:
    telemetry: 
      http-port: 9090
```

