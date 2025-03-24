---
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Set Up with Helm

This section provides a quick guide to use SeaTunnel with Helm.

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

## Submit Job

The default config doesn't enable ingress, so you need forward the master restapi.
```bash
kubectl port-forward -n default svc/seatunnel-master 5801:5801
```
Then you can access restapi with "http://127.0.0.1/5801/"

If you want to use ingress, update `value.yaml`

for example:
```commandline
ingress:
  enabled: true
  host: "<your domain>"
```
Then upgrade seatunnel.

Then you can access restapi with `http://<your domain>`

Or you can just go into master pod, and use local curl command.
```commandline
# get one of the master pods
MASTER_POD=$(kubectl get po -l  'app.kubernetes.io/name=seatunnel-master' | sed '1d' | awk '{print $1}' | head -n1)
# go into master pod container.
kubectl -n default exec -it $MASTER_POD -- /bin/bash

curl http://127.0.0.1:5801/running-jobs
curl http://127.0.0.1:5801/system-monitoring-information
```

After that you can submit your job by [rest-api-v2](../../seatunnel-engine/rest-api-v2.md)

## What's More

For now, you have taken a quick look at SeaTunnel, and you can see [connector](../../connector-v2/source) to find all sources and sinks SeaTunnel supported.
Or see [deployment](../../seatunnel-engine/deployment.md) if you want to submit your application in another kind of your engine cluster.
