# Connector Isolated Dependency Loading Mechanism

SeaTunnel provides an isolated dependency loading mechanism for each connector, making it easier for users to manage individual dependencies for different connectors, while avoiding dependency conflicts and improving system extensibility.
When loading a connector, SeaTunnel will search for and load the connector's own dependency jars from the `${SEATUNNEL_HOME}/plugins/connector-xxx` directory. This ensures that the dependencies required by different connectors do not interfere with each other, which is helpful for managing a large number of connectors in complex environments.

## Principle

Each connector needs to place its own dependency jars in a dedicated subdirectory under `${SEATUNNEL_HOME}/plugins/connector-xxx` (manual creation required).
The subdirectory name is specified by the value in the `plugin-mapping` file. When SeaTunnel starts and loads connectors, it will only load jars from the corresponding directory, thus achieving dependency isolation.

Currently, the Zeta engine ensures that jars for different connectors in the same job are loaded separately. The other two engines still load all connector dependency jars together, so placing different versions of jars for the same job in Spark/Flink environments may cause dependency conflicts.

## Directory Structure Example

- Use `${SEATUNNEL_HOME}/connectors/plugin-mapping.properties` to get the folder name for each connector.

For example, for AmazonDynamodb, suppose the following configuration exists in the `plugin-mapping` file:
```
seatunnel.source.AmazonDynamodb = connector-amazondynamodb
```

The corresponding connector dependency directory is the value `connector-amazondynamodb`.

The final directory structure is as follows:

```
SEATUNNEL_HOME/
  plugins/
    connector-amazondynamodb/
      dependency1.jar
      dependency2.jar
    connector-xxx/
      dependencyA.jar
      dependencyB.jar
```

## Limitations

- For the Zeta engine, please ensure that the `${SEATUNNEL_HOME}/plugins/connector-xxx` directory structure is consistent across all nodes. Each node must contain the same subdirectories and dependency jars.
- Any directory or jar that does not start with `connector-` will be treated as a common dependency directory, and all engines and connectors will load such jars.
- In the Zeta engine, you can achieve shared dependencies for all connectors by placing common jars in the `${SEATUNNEL_HOME}/lib/` directory.

## Verification

- By checking the job logs, you can confirm that each connector only loads its own dependency jars.

    ```log
    2025-08-13T17:55:48.7732601Z [] 2025-08-13 17:55:47,270 INFO  org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery - find connector jar and dependency for PluginIdentifier{engineType='seatunnel', pluginType='source', pluginName='Jdbc'}: [file:/tmp/seatunnel/plugins/Jdbc/lib/vertica-jdbc-12.0.3-0.jar, file:/tmp/seatunnel/connectors/connector-jdbc-2.3.13-SNAPSHOT-2.12.15.jar]
    ```
