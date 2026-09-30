# SeaTunnel resource managers

> Application mode is an experimental feature.

Optional YARN and Kubernetes providers for native Zeta application mode. Each application runs one dedicated master and a fixed number of workers.

```text
seatunnel-resource-managers/
├── pom.xml                       # Parent and aggregator; inherits the repository root
├── yarn/                         # Maven artifact: seatunnel-resource-manager-yarn
│   ├── pom.xml
│   └── src/                      # Entrypoint plus config/, client/ and cluster/
└── kubernetes/                   # Maven artifact: seatunnel-resource-manager-kubernetes
    ├── pom.xml
    └── src/                      # Entrypoint plus config/, client/ and cluster/
```

There is no resource-manager `core` module. Shared `ApplicationSpecification` and `WorkerSpecification` live in engine-common under `org.apache.seatunnel.engine.common.config.spec`; `ApplicationOptions` lives in `engine.common.config.server` and `SeatunnelApplicationConfig` in `engine.common.config`. `ApplicationJarPathResolver` lives in engine-core. These modules do not depend on platform SDKs.

Engine client owns native Zeta job communication, the deployment SPI and `ApplicationJobExecutionEnvironment`. Engine server owns the external worker-driver SPI, resource lifecycle and existing slot scheduler. Platform providers implement deployment and worker drivers; separate Master/Worker CLIs prepare configuration and call `SeaTunnelServerStarter.createHazelcastInstance`. Master CLIs own job cancellation signaling and master shutdown.

```text
engine-common → seatunnel-api
engine-core → engine-common, seatunnel-core-starter
engine-server → engine-core
engine-client → engine-server
seatunnel-starter → engine-client, engine-server
yarn / kubernetes → engine-client, engine-server
seatunnel-dist → seatunnel-starter, yarn, kubernetes
```

The deployment-provider SPI resource is `META-INF/services/org.apache.seatunnel.engine.client.deployment.ApplicationClusterDescriptorFactory`.

Arrows mean “depends on”. The root reactor has one `seatunnel-resource-managers` module entry. Its parent POM aggregates `yarn` and `kubernetes`; both inherit this parent. CI explicitly selects `ci` to exclude distribution packaging.

To build the standard SeaTunnel distribution with both providers:

```shell
./mvnw -Prelease,seatunnel -pl seatunnel-dist -am \
  -DskipTests -Dskip.ui=true package
```

The standard archive is `seatunnel-dist/target/apache-seatunnel-${version}-bin.tar.gz`. Provider bundles are placed in `resource-managers/yarn` and `resource-managers/kubernetes` inside the distribution, and the launcher loads only the selected platform.

The unified E2E module uses Maven dependencies to prepare its test runtime, following the existing connector E2E staging mechanism:

```shell
./mvnw -T 1 -B verify -DskipUT=true -DskipIT=false \
  -D"license.skipAddThirdParty"=true -D"skip.ui"=true --no-snapshot-updates \
  -pl :seatunnel-resource-managers-e2e -am -Pci
```

At `process-test-resources`, `maven-dependency-plugin:copy` stages both platform providers and their runtime artifacts under `target/test-classes/e2e-dependencies`. Tests resolve these files through the shared `DependencyJar` utility and prepare a temporary SeaTunnel directory with the repository configuration and scripts. Kubernetes builds its image from this directory; YARN compresses the directory for container localization. No prebuilt distribution or additional assembly descriptor is required. Both platform suites read the same FakeSource, Console and Assert jobs from `src/test/resources/common`; platform directories contain only Engine configuration. Follow the platform guides for running the integration tests.

Persistent checkpoints use the existing Engine storage configuration, including HDFS and object stores. A new application can restore a previous job's checkpoint using its native job ID. This recovery path does not require Hazelcast backup replicas; automatic master failover is not provided.
