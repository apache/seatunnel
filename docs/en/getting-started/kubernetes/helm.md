---
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Set Up with Helm

This section provides a quick guide to use SeaTunnel with Helm.

:::tip

In Zeta cluster mode, both Master and Worker should be deployed with StatefulSet for production. The current Helm chart is suitable for a quick start, but it still renders Master and Worker as Deployment. Before using Helm in production, read [Kubernetes Deployment](kubernetes.mdx), [Separated Cluster Mode](separated-cluster-mode.md), and [Kubernetes Configuration](configuration.md) to understand the StatefulSet-based topology, Headless Service, checkpoint, IMap MapStore, and slot planning practices.

:::

## Prerequisites

We assume that you have one local installation as follow:

- [docker](https://docs.docker.com/)
- [kubernetes](https://kubernetes.io/)
- [helm](https://helm.sh/docs/intro/quickstart/)

So that the `kubectl` and `helm` commands are available on your local system.

Take kubernetes [minikube](https://minikube.sigs.k8s.io/docs/start/) as an example, you can start a cluster with the following command:

```bash
minikube start --kubernetes-version=v1.23.3
```

## Install

Install with default settings.
```bash
# Choose the corresponding version yourself
export VERSION=2.3.10
helm pull oci://registry-1.docker.io/apache/seatunnel-helm --version ${VERSION}
tar -xvf seatunnel-helm-${VERSION}.tgz
cd seatunnel-helm
helm install seatunnel .
```
Install with another namespace.
```bash
helm install seatunnel . -n <your namespace>
```

For managed Kubernetes services, keep provider-specific changes in a separate values file and pass it with `-f`, for example:

```bash
helm install seatunnel . -n <your namespace> -f values-eks.yaml
```

Common managed-cluster values to review include:

- image repository and `imagePullSecrets` for ECR, Artifact Registry, ACR, ACK Container Registry, TCR, SWR, Volcengine Container Registry, or an OpenShift internal registry
- ServiceAccount, RBAC, and OpenShift SecurityContextConstraint requirements for the target namespace
- Service or Ingress annotations for the provider load balancer, subnet, certificate, and internal/external exposure model
- object storage or PersistentVolume settings for checkpoint and state paths
- resource requests, limits, node selectors, tolerations, and affinity rules for the node pools that should run SeaTunnel pods
- log and metrics collection settings for the provider monitoring stack

## Submit Job

The default configuration does not enable Ingress, so forward the Master REST API port first.
```bash
kubectl port-forward -n default svc/seatunnel-master 8080:8080
```
Then you can access REST API with `http://127.0.0.1:8080/`.

If you want to use Ingress, update `values.yaml`.

for example:
```commandline
ingress:
  enabled: true
  host: "<your domain>"
```
Then upgrade SeaTunnel.

Then you can access REST API with `http://<your-domain>`.

Or you can just go into master pod, and use local curl command.
```commandline
# get one of the master pods
MASTER_POD=$(kubectl get po -l  'app.kubernetes.io/name=seatunnel-master' | sed '1d' | awk '{print $1}' | head -n1)
# go into master pod container.
kubectl -n default exec -it $MASTER_POD -- /bin/bash

curl http://127.0.0.1:8080/running-jobs
curl http://127.0.0.1:8080/system-monitoring-information
```

After that, submit jobs through [REST API V2](../../engines/zeta/rest-api-v2.md).

## What's More

For now, you have taken a quick look at SeaTunnel, and you can see [connector](../../connectors/source) to find all sources and sinks SeaTunnel supported.
For handwritten Kubernetes manifests and production deployment recommendations, see [Separated Cluster Mode](separated-cluster-mode.md) and [Kubernetes Operations](operations.md).
