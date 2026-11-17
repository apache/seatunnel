# SeaTunnel Edge Agent

SeaTunnel Edge Agent is a lightweight collector for local data such as log files on edge hosts.
It buffers records in a local write-ahead log (WAL) and sends them to SeaTunnel Engine through
EdgeSocket, with retries when delivery fails.

## Build from source

Build the Edge Agent package from the SeaTunnel source root:

```bash
./mvnw clean package -pl seatunnel-dist -am -DskipTests -Dskip.spotless=true
```

The build generates the binary package:

```text
seatunnel-dist/target/apache-seatunnel-edge-agent-<version>-bin.tar.gz
```
