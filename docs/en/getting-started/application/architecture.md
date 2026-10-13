---
sidebar_position: 2
title: Architecture
---

# Application Mode architecture

Application Mode adds a resource-manager layer to the Engine. Everything below the layer is the
regular Zeta runtime: worker registration, slot scheduling, task execution, failure propagation and
checkpoints. The platform provider only starts, observes and reclaims the processes, and maps the
application result to the platform state.

## Engine side

```text
engine-client   deployment SPI: ApplicationClusterDescriptorFactory → ClusterDescriptor
engine-server   ApplicationResourceManager, ResourceManagerDriver SPI, ApplicationJobRunner
engine-common   ApplicationSpecification, WorkerSpecification, application options
```

- `ResourceManagerFactory` creates either the `StandaloneResourceManager` or the
  `ApplicationResourceManager` for one application. It receives the application ID, the
  `ApplicationSpecification` and the platform driver built by the application entrypoint.
- `ApplicationResourceManager` owns the driver lifecycle: initialization, waiting for the fixed
  worker set, resource failure notification, worker release, terminal-result publication and driver
  close. Callbacks are dispatched on the main-thread executor, platform SDK work on the IO executor.
- `ResourceManagerDriver` is the only contract a platform provider implements for resource
  lifecycle: initialize(handler, main executor, IO executor, master address), requestWorker,
  releaseWorker and close. The driver never runs the job and never mixes platform SDK types into
  the Engine.
- `ApplicationJobRunner` waits for worker registration, runs exactly one job, cancels it on
  interruption or resource failure, then releases workers and publishes the terminal result.

## Submission side

- `ApplicationClusterDescriptorFactory` is discovered through the deployment SPI and selects the
  provider for a target. It validates the target, opens the platform connection and returns a
  `ClusterDescriptor`.
- `ClusterDescriptor` deploys the application and returns the platform-native ID, discovers the
  running master, queries the durable platform state and cancels the application. None of these
  operations depend on a live Engine client.
- `ApplicationClusterDeployer` loads the provider, calls `deployApplication` and closes the local
  descriptor without stopping the remote application.

## Specification and localization

- The submitter loads the deployment file and the job file separately. Deployment options and the
  immutable `ApplicationSpecification` stay independent; the specification contains only the
  resolved application fields that the master needs.
- The job file is carried to the master verbatim. The master parses it once, resolves every
  `${...}` placeholder against its own environment (including platform-injected variables) and
  executes the result.
- The localized specification travels through the platform mechanism, for example mounted configuration files or injected environment variables; the master reconstructs the parameters from its environment and mounted files.
- Application-owned Secrets and ConfigMaps are created after the owner resource and carry its owner
  reference, so cleanup removes exactly what the application created and never touches user-owned
  objects.

## Lifecycle

1. The submitter validates options and user-managed configuration, creates the owner application and
   its accompanying resources, then starts it. Submission does not wait for the master.
2. The master starts its Zeta member, creates the fixed workers through the driver, and waits for
   their registration within the configured startup timeout.
3. The master resolves the job file and executes exactly one job. Worker loss or resource failure
   fails the application; workers are not replenished.
4. After the job terminates the master releases the workers, publishes the result, and exits. The
   platform records the terminal state and collects the remaining resources.

Checkpoint storage is engine-owned and independent of the application lifecycle, so a later
application can restore the previous job by ID.

Platform-specific details such as custom mounts, environment variables and runtime configuration
are documented in the platform guide.
