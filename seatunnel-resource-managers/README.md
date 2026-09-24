# SeaTunnel resource managers

> Application mode is an experimental feature.

Optional YARN and Kubernetes providers for native Zeta application mode. Each application runs one dedicated master and a fixed number of workers.

```text
seatunnel-resource-managers/
├── pom.xml                       # Parent and aggregator; inherits the repository root
├── core/                         # Default module: seatunnel-resource-manager-core
│   ├── pom.xml                   # Inherits seatunnel-resource-managers
│   └── src/main/java/.../resource/core/
│       ├── application/          # Application and worker models
│       ├── client/               # Deployment lifecycle contracts and provider discovery
│       ├── config/               # Deployment options, membership and checkpoint configuration
│       └── classloader/          # Localized distribution jar resolver
├── yarn/                         # Maven artifact: seatunnel-resource-manager-yarn
│   ├── pom.xml
│   └── src/                      # Entrypoint plus config/, client/ and cluster/
└── kubernetes/                   # Maven artifact: seatunnel-resource-manager-kubernetes
    ├── pom.xml
    └── src/                      # Entrypoint plus config/, client/ and cluster/
```

`core` is part of the default reactor. Under `org.apache.seatunnel.resource.core`, `application` contains the five application/worker model types, `client` contains the four deployment/client contracts and discovery types, `config` contains `ApplicationOptions` and `ApplicationClusterConfig`, and `classloader` contains `ApplicationJarPathResolver`. It depends on Engine common to reuse the existing `DeployType`, and on Engine core for the typed `JarPathResolver` extension. It does not depend on Engine server, Engine client or platform SDKs. Engine common and Engine core have no reverse dependency on this module. Engine core uses the generic `seatunnel-core-starter` artifact, which is separate from `seatunnel-starter`.

Engine client retains native Zeta job communication over Hazelcast. Engine server owns the external worker-driver SPI and the existing slot scheduler. `ApplicationRuntime` and `ApplicationWorker` stay in starter because they create Engine members and own native server/client lifecycles. Platform providers implement the core deployment SPI and the Engine worker-driver SPI, without replacing slot scheduling.

```text
resource-manager-core → engine-common → seatunnel-api
resource-manager-core → engine-core → seatunnel-core-starter
engine-server → resource-manager-core
seatunnel-starter → resource-manager-core, engine-client, engine-server
yarn / kubernetes → resource-manager-core, seatunnel-starter
seatunnel-dist → seatunnel-starter, yarn, kubernetes
```

Inside core, `client` uses `application` models, and `application` uses `config` options. The deployment-provider SPI resource is `META-INF/services/org.apache.seatunnel.resource.core.client.ApplicationDeployerFactory`.

Arrows mean “depends on”. The root reactor has one `seatunnel-resource-managers` module entry. Its parent POM aggregates `core`, `yarn`, and `kubernetes`, and all three children inherit this parent. CI explicitly selects `ci` to exclude distribution packaging.

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
